from os import listdir
from os.path import isfile, join, isdir
import sys
import os

def replace_ton_api(filedata, to_local):
    if to_local:
        r_from = 'git = "ssh://git@github.com/tonlabs/TON-Transport.git", package = "ton_api"'
        r_to = 'path = "../TON-Transport/ton_api"'
    else:
        r_to = 'git = "ssh://git@github.com/tonlabs/TON-Transport.git", package = "ton_api"'
        r_from = 'path = "../TON-Transport/ton_api"'
    new_filedata = filedata.replace(r_from, r_to)
    if (new_filedata == filedata) and to_local:
        r_from = 'git = "ssh://git@github.com/tonlabs/TON-Transport", package = "ton_api"'
        new_filedata = filedata.replace(r_from, r_to)
    return new_filedata

def replace(full_name, replaced, to_local, depth):
    with open(full_name, 'r') as file:
        filedata = file.read()
    for crate in replaced:
        if to_local:
            r_from = 'git = "ssh://git@github.com/tonlabs/' + crate + '.git"'
            r_to = 'path = "' + ('../' * depth) + crate + '"'
        else:
            r_to = 'git = "ssh://git@github.com/tonlabs/' + crate + '.git"'
            r_from = 'path = "' + ('../' * depth) + crate + '"'
        new_filedata = filedata.replace(r_from, r_to)
        if (new_filedata == filedata) and to_local:
            r_from = 'git = "ssh://git@github.com/tonlabs/' + crate + '"'
            new_filedata = filedata.replace(r_from, r_to)
        filedata = new_filedata
    filedata = replace_ton_api(filedata, to_local)
    with open(full_name, 'w') as file:
        file.write(filedata)

def process_dir(path, replaced, to_local, depth):
    for f in listdir(path):
        full_name = join(path, f)
        if isfile(full_name) and f.lower() == 'cargo.toml':
            replace(full_name, replaced, to_local, depth)
        elif isdir(full_name):
            process_dir(full_name, replaced, to_local, depth + 1)

if len(sys.argv) < 2 or (sys.argv[1] != 'to-local' and sys.argv[1] != 'to-git'):
    print("""
This script replaces strings like 
    git = "ssh://git@github.com/tonlabs/some-crate.git"
to
    path = "../some-crate"
(and vice versa) for hardcoded list of crates.
To use local repositories:
clone all node's repos into one directory
    ton-node
        TON-Transport
        adnl
        dht
        overlay
        rldp
        ton-block
        ton-block-json
        ton-node
        ton-node-storage
        ton-types
        ton-vm
and run
    python3 change-deps.py to-local ..

usage: python3 change-deps.py <to-local|to-git> [directory]""")
    exit(1)

if len(sys.argv) == 3:
    dir = sys.argv[2]
else:
    dir = '.'

replaced = [
    'adnl',
    'dht',
    'ton-node-storage',
    'ton-block-json',
    'ton-block',
    'ton-types',
    'overlay',
    'rldp',
]
process_dir(dir, replaced, sys.argv[1] == 'to-local', 0)