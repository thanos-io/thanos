package ui

import "testing"

func TestSanitizePrefix(t *testing.T) {
	type args struct {
		prefix string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"InvalidEscaping",
			args{
				prefix: "/%%",
			},
			"",
			true,
		},
		{
			"URL",
			args{
				prefix: "http://www.example.com/some%20path/two?foo=bar?foo1=bar1#id",
			},
			"/some path/two",
			false,
		},
		{
			"DelimiterNotAllowed",
			args{
				prefix: "http://www.example.com/host%3A%2F%2Fpath/",
			},
			"/host:/path",
			false,
		},
		{
			"EmptyPrefix",
			args{
				prefix: "",
			},
			"",
			false,
		},
		{
			"Root",
			args{
				prefix: "/",
			},
			"",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizePrefix(tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizePrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SanitizePrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}
