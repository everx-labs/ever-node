import os
import urllib3
import argparse
import requests
import subprocess

build_url = "https://builder.tonlabs.io/jenkins/job/ton-node/job/master/lastSuccessfulBuild/buildNumber"
artifact_url = 'https://builder.tonlabs.io/jenkins/job/ton-node/job/master/{}/artifact/tonlabs.zip'

def main(args):
    if not os.path.exists(args.credentials):
        print("[-] Invalid path to credentials")
        return 

    if args.source == 'git':
       download_github(args.credentials)
    else:
       download_jenkins(args.credentials)
    
    
def download_github(creds):
    os.mkdir('/tonlabs')
    os.chdir('/tonlabs')
    subprocess.run(['ssh-agent', '-s'])
    subprocess.run(['ssh-add', creds])
    subprocess.run(['git', 'clone', 'git@github.com:tonlabs/ton-node.git'])


def download_jenkins(credentials):
    def get_creds(filepath):
        with open(filepath, "r") as f:
            uname_pass = f.readline()
            if uname_pass.find(':') != -1:
                return uname_pass.split(':')
            else:
                print("[-] Invalid jenkins credentials file")
                return ("","")
    
    username,token = get_creds(credentials)

    build_num_req = requests.get(_url_bn, auth=(username, token))
    if build_num_req.status_code != 200:
        print("Request error")
        return

    build_num = build_num_req.text
    download_url = artifact_url.format(build_num)
    zip_req = requests.get(donwload_url, auth=(username, token))
    if zip_req.status_code != 200:
        print("Request error")
        return
    
    with open("tonlabs.zip", "wb") as f:
        f.write(zip_req.content)
    suprocess.run(['unzip', 'tonlabs.zip', '-d', '/'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='download ton-node sources')
    parser.add_argument('-c', '--cred', type=str, dest='credentials', help='Path to file with credentials (private key for git and login:pass for jenkins)')
    parser.add_argument('source', type=str, choices=['git', 'jenkins'], help='git repository / jenkins artifacts')
    args = parser.parse_args()
    main(args)
