# s3sync

s3sync is a simple tool to sync the files in a directory with the files in an s3 bucket. A key prefix can be configured to target only a part of a bucket. More than one directory can be synced with the same bucket, but not at the same time. In other words, this tool is intended to be used by a single user possibly working on multiple devices.

s3sync is useful in two kinds of scenarios
- Files are binary and / or source control systems would not add much value. An example of this is syncing build artifacts for a static website.
- Maintaining history is not useful or is actually undesirable. An example of this is personal notes.

s3sync avoids making unnecessary requests to the bucket for files that have not changed on either side. The logic for doing this assumes that the files in the bucket will not be modified by any other tool.

## Usage

Run `s3ync --init directory` on an existing empty directory and fill in the config.xtn file that gets created within the .s3sync directory.

Subsequently, run `s3sync directory` to sync files. Whenever possible, run this command before making any changes to any files in the directory, and run it afterwards to push the changes to the bucket. Following this practice will ensure that there are no sync conflicts. If changes have been pushed into the bucket from another device, and there are also local changes in the same files, there will be conflicts when the local files are synced. Conflicts must be resolved manually even if the changes are in different lines.

If there is a conflict in syncing file `filename.ext`, a conflict resolution file with a name of `filename.conflict.remote.ext` or `filename.conflict.delete.ext` gets created in the same directory as `filename.ext`. Compare this file with the original file using any diff tool, make changes to it as necessary and then delete the .conflict file. Run the tool again to complete the sync. `filename.conflict.remote.ext` gets created when there is a modification in the s3 bucket, and the local file has also been modified or has been deleted. `filename.conflict.delete.ext` gets created when the file has been deleted from the s3 bucket, and the local file has been modified. `filename.conflict.delete.ext` is always empty. It is just a conflict marker.

## Logic

A status file is maintained in the s3 bucket that holds a version number. A version number is also maintained in the local directory. At the end of a successful sync, the numbers will be the same. If there is a sync from a different device that results in modifications to the bucket, the version number in the bucket will be incremented and the local version number on the current device will then be behind. A list of keys and actions (put or delete) for changed files for each version number is maintained in the bucket. During a sync, these listings are used to detect changes made in the bucket.

Whenever s3sync is run, it creates / updates an index file in the .s3sync directory. This file tracks the modification time of local files when they were last known to be in sync with the bucket. If the modification time of a local file is different from the one maintained in the index, or there is no record of the file in the index, it is assumed that the file has been changed or added, and it will be uploaded to the bucket on the next sync. If there is a record in the index, but the file no longer exists locally, it is assumed that the file has been deleted, and it will be deleted from the bucket on the next sync.

If an index file is missing, and the local directory is empty, all files in the bucket are downloaded to the directory, and the index is created.

If the version in the local directory matches the version in the bucket, it is assumed that there are no new changes in the bucket, and any changes made locally (determined by comparing the index to the files) are replicated on the bucket by uploading or deleting files in the bucket.

If the version in the local directory is behind the version in the bucket, all the changelists for newer versions are aggregated, and the changes are replicated in the directory by downloading files from the bucket or deleting local files. However, if these files have also been modified locally, there will be conflicts. A file modified remotely is available as `filename.conflict.remote.ext` as described in the usage section. When there is a conflict, the etag of the file in the bucket is stored in the index. After the conflict file is deleted and a sync is attempted again, the conflict is considered to have been resolved if the etag in the index still matches the etag in the bucket. If not, the latest file in the bucket is downloaded again and stored as `filename.conflict.remote.ext`.

## Limitations
The tool cannot track file renames or moves. These operations will be treated as deletion of the old file and creation of a new file.
