sphinx-apidoc -f -o docs mreduce
pushd docs
make html
popd
