#!/bin/sh -eu

if [ $# -ge 1 ]
then
  files=$*
else
  files="scone tests"
fi

echo "Linting these locations: $files"
echo " ===== Running isort ===== "
isort $files
echo " ===== Running black ===== "
black $files
echo " ===== Running flake8 ===== "
flake8 $files
echo " ===== Running mypy ===== "
mypy $files
