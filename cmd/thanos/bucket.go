package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/template"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/improbable-eng/thanos/pkg/verifier"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	issuesMap = map[string]verifier.Issue{
		verifier.IndexIssueID:            verifier.IndexIssue,
		verifier.OverlappedBlocksIssueID: verifier.OverlappedBlocksIssue,

		// If you know what you are doing!
		verifier.DuplicatedCompactionIssueID: verifier.DuplicatedCompactionIssue,
		verifier.SeparatedBlocksID:           verifier.SeparatedBlocks,
	}
	allIssues = func() (s []string) {
		for id := range issuesMap {
			s = append(s, id)
		}
		return s
	}
)

func registerBucket(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "inspect metric data in an object storage bucket")

	gcsBucket := cmd.Flag("gcs-bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").String()

	s3Config := s3.RegisterS3Params(cmd)

	// Verify command.
	verify := cmd.Command("verify", "verify all blocks in the bucket against specified issues")
	verifyRepair := verify.Flag("repair", "attempt to repair blocks for which issues were detected").
		Short('r').Default("false").Bool()
	// NOTE(bplotka): Currently we support backup buckets only in the same project.
	verifyBackupGCSBucket := cmd.Flag("gcs-backup-bucket", "Google Cloud Storage bucket name to backup blocks on repair operations.").
		PlaceHolder("<bucket>").String()
	verifyBackupS3Bucket := cmd.Flag("s3-backup-bucket", "S3 bucket name to backup blocks on repair operations.").
		PlaceHolder("<bucket>").String()
	verifyIssues := verify.Flag("issues", fmt.Sprintf("issues to verify (and optionally repair). Possible values: %v", allIssues())).
		Short('i').Default(verifier.IndexIssueID, verifier.OverlappedBlocksIssueID).Strings()
	m[name+" verify"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer) error {
		bkt, closeFn, err := client.NewBucket(gcsBucket, *s3Config, reg)
		if err != nil {
			return err
		}

		backupS3Config := *s3Config
		backupS3Config.Bucket = *verifyBackupS3Bucket
		backupBkt, backupCloseFn, err := client.NewBucket(verifyBackupGCSBucket, backupS3Config, reg)
		if err == client.ErrNotFound {
			if *verifyRepair {
				return errors.Wrap(err, "repair is specified, so backup client is required")
			}
			// No repair - no need for backup bucket.
			backupCloseFn = func() error { return nil }

		} else if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer closeFn()
		defer backupCloseFn()

		var (
			ctx    = context.Background()
			v      *verifier.Verifier
			issues []verifier.Issue
		)

		for _, i := range *verifyIssues {
			issueFn, ok := issuesMap[i]
			if !ok {
				return errors.Errorf("no such issue name %s", i)
			}
			issues = append(issues, issueFn)
		}

		if *verifyRepair {
			v = verifier.NewWithRepair(logger, bkt, backupBkt, issues)
		} else {
			v = verifier.New(logger, bkt, issues)
		}

		return v.Verify(ctx)
	}

	ls := cmd.Command("ls", "list all blocks in the bucket")
	lsOutput := ls.Flag("output", "format in which to print each block's information; may be 'json' or custom template").
		Short('o').Default("").String()
	m[name+" ls"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer) error {
		bkt, closeFn, err := client.NewBucket(gcsBucket, *s3Config, reg)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer closeFn()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		var (
			format     = *lsOutput
			printBlock func(id ulid.ULID) error
		)

		switch format {
		case "":
			printBlock = func(id ulid.ULID) error {
				fmt.Fprintln(os.Stdout, id.String())
				return nil
			}
		case "json":
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "\t")

			printBlock = func(id ulid.ULID) error {
				m, err := block.DownloadMeta(ctx, bkt, id)
				if err != nil {
					return err
				}
				return enc.Encode(&m)
			}
		default:
			tmpl, err := template.New("").Parse(format)
			if err != nil {
				return errors.Wrap(err, "invalid template")
			}
			printBlock = func(id ulid.ULID) error {
				m, err := block.DownloadMeta(ctx, bkt, id)
				if err != nil {
					return err
				}

				if err := tmpl.Execute(os.Stdout, &m); err != nil {
					return errors.Wrap(err, "execute template")
				}
				fmt.Fprintln(os.Stdout, "")
				return nil
			}
		}

		return bkt.Iter(ctx, "", func(name string) error {
			id, ok := block.IsBlockDir(name)
			if !ok {
				return nil
			}
			return printBlock(id)
		})
	}
}
