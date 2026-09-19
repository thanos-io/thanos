The `ui` package contains static files and templates used in the web UI. For
easier distribution they are included in the Thanos binary using go:embed.

During development it is more convenient to always use the files on disk to
directly see changes without recompiling.
Set the environment variable `DEBUG=1` and compile Thanos for this to work.
This is for development purposes only.

After making changes to any file, run `make react-app` before committing to update
the generated static files and templates.
