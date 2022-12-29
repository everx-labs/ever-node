#!/bin/bash
export DOCKER_BUILDKIT=1 ; docker build --tag rustnode:local --ssh default .