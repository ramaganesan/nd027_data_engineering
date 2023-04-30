import argparse
import configparser
import json
import logging
import os
import pandas as pd

import boto3

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
REGION = config.get("AWS", "REGION")

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

S3_READ_POLICY = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

ec2 = boto3.resource('ec2',
                     region_name=REGION,
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )

s3 = boto3.resource('s3',
                    region_name=REGION,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam', aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name=REGION
                   )

redshift = boto3.client('redshift',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )


def create_iam_role(iam):
    """ Creating IAM role for Redshift cluster """
    try:
        dwh_role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn=S3_READ_POLICY
        )
    except Exception as e:
        logging.warning(e)

    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.info('Role {} with arn {}'.format(DWH_IAM_ROLE_NAME, role_arn))
    return role_arn


def create_redshift_cluster(redshift, role_arn):
    """ Creating Redshift cluster """
    try:
        redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[role_arn],
        )
        logging.info('Creating cluster {}...'.format(DWH_CLUSTER_IDENTIFIER))
    except Exception as e:
        logging.error(e)


def delete_iam_role(iam):
    """ Deleting IAM role """
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=S3_READ_POLICY)
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    logging.info('Successfully Deleted role {} with {}'.format(DWH_IAM_ROLE_NAME, role_arn))


def delete_redshift_cluster(redshift):
    """ Deleting Redshift cluster """
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True,
        )
        logging.info('Deleted cluster {}'.format(DWH_CLUSTER_IDENTIFIER))
    except Exception as e:
        logging.error(e)


def open_tcp(ec2, vpc_id):
    """ Open TCP connection for Redshift Cluster """
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
        logging.info('Allow TCP connections from everywhere')
    except Exception as e:
        logging.error(e)


def redshift_cluster_properties(props):
    """Reads properties of Redshift Cluster into a Panda Dataframe"""
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def describe_redshift_cluster():
    """Waits until the cluster is created and get information about the Redshift Cluster"""
    try:
        waiter = redshift.get_waiter('cluster_available')
        waiter.wait(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MaxRecords=20,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 30
            }
        )
        return redshift.describe_clusters()['Clusters'][0]
    except Exception as e:
        logging.error(e)


def main(args):
    """ Main function """
    if args.option == "delete":
        delete_redshift_cluster(redshift)
        delete_iam_role(iam)
    elif args.option == "create":
        role_arn = create_iam_role(iam)
        create_redshift_cluster(redshift, role_arn)

        cluster_properties = describe_redshift_cluster()
        DWH_ENDPOINT = cluster_properties['Endpoint']['Address']
        logging.info('Cluster created at {}'.format(DWH_ENDPOINT))

        open_tcp(ec2, cluster_properties['VpcId'])

    elif args.option == "opentcp":
        cluster_properties = describe_redshift_cluster()
        logging.info(cluster_properties)

        DWH_ENDPOINT = cluster_properties['Endpoint']['Address']
        logging.info('Cluster created at {}'.format(DWH_ENDPOINT))
        open_tcp(ec2, cluster_properties['VpcId'])
    else:
        logging.error('Invalid Argument to run the program ')


if __name__ == '__main__':
    """ Set logging level and cli arguments """
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--option', dest='option', type=str, help='create, delete, opentcp Redshift cluster',
                        default=False)
    args = parser.parse_args()
    main(args)
