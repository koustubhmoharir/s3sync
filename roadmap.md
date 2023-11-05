# Roadmap

- Support for excluding files or sub-directories from the sync. This is needed for use in deployment scenarios where several files / directories get produced as part of a build but should not be deployed.
- Keeping a local backup of files whenever a file is overwritten or deleted, either in the bucket or locally.
- UI support to resolve conflicts more easily and to launch a pre-installed diff tool.
- Support for dealing with all the failures that may happen during uploads / downloads.
- Multi-part uploads for large files.