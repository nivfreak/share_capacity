A small script to log the capacity usage for all SMB shares, NFS exports, and S3 buckets on a cluster.

This script will require a hostname and access token (https://docs.qumulo.com/administrator-guide/connecting-to-external-services/creating-using-access-tokens-to-authenticate-external-services-qumulo-core.html) be specified in the yaml ".config" file, or via commandline arguements. See example.config. 

To created a limited Role for this script, we will require the following: PRIVILEGE_FS_ATTRIBUTES_READ, PRIVILEGE_NFS_EXPORT_READ, PRIVILEGE_SMB_SHARE_READ, PRIVILEGE_S3_BUCKETS_READ
