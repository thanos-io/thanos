The `ui` package contains static files and templates used in the web UI. For
easier distribution they are statically compiled into the Thanos binary
using `embed` package.

During development it is more convenient to always use the files on disk to
directly see changes without recompiling.
Set the environment variable `DEBUG=1` and compile Thanos for this to work.
This is for development purposes only.
