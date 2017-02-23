from boto3.session import Session
from botocore.exceptions import ClientError


def run(sessionObj, params):
    response = {
        'metaData': {},
        'status': 'failure',
        'detail': 'Execution Detail for the job'
    }
    if sessionObj is not None and params is not None:

        # Write your script here
        try:
            response['metaData']['snapshotList'] = []
            response['metaData']['UnCopiedVolumeList'] = []
            if 'sourceRegion' not in params or 'tag-key' not in params or 'tag-value' not in params or 'destRegion' not in params:
                response['status'] = 'error'
                response['detail'] = 'Parameter validation failed'
            else:
                region = params['sourceRegion']
                destRegion = params['destRegion']
                tagKey = params['tag-key']
                tagValue = params['tag-value']
                conn = sessionObj.client(service_name='ec2', region_name=region)
                destConn = sessionObj.client(service_name='ec2', region_name=destRegion)
                snapshots = conn.describe_snapshots(OwnerIds=['self'])['Snapshots']
                snapshots.sort(cmp=lambda x, y: cmp(x['StartTime'], y['StartTime']), reverse=True)
                volumeSnapshotMapping = {}
                for snapshot in snapshots:
                    if snapshot['VolumeId'] not in volumeSnapshotMapping:
                        volumeSnapshotMapping[snapshot['VolumeId']] = snapshot['SnapshotId']

                reservations = conn.describe_instances(Filters=[
                    {
                        'Name': 'tag:{}'.format(tagKey),
                        'Values': [
                            tagValue
                        ]
                    },
                    {
                        'Name': 'root-device-type',
                        'Values': ['ebs']
                    }
                ])
                instances = [instance for reservation in reservations['Reservations'] for instance in
                             reservation['Instances']]
                for instance in instances:
                    description = "{}:{}".format("Copied by Custom Lambda for Instance", instance['InstanceId'])
                    blockDeviceMappings = instance['BlockDeviceMappings']
                    for deviceMapping in blockDeviceMappings:
                        if 'Ebs' in deviceMapping:
                            volumeId = deviceMapping['Ebs']['VolumeId']
                            if volumeId in volumeSnapshotMapping:
                                sourceSnapshot = volumeSnapshotMapping[volumeId]
                                snapshot = destConn.copy_snapshot \
                                    (SourceSnapshotId=sourceSnapshot,
                                     Description=description,
                                     SourceRegion=region
                                     )
                                response['metaData']['snapshotList'].append(snapshot['SnapshotId'])
                            else:
                                response['metaData']['UnCopiedVolumeList'].append(volumeId)
                response['status'] = 'success'
                response['detail'] = "Snapshots are copied"
        except ClientError as e:
            response['status'] = 'error'
            response['detail'] = str(e.message)
    else:
        response['status'] = 'error'
        response['detail'] = 'Required parameters missing'

    return response
