#!/bin/bash

for subfolder in smap dataframes epwformat
do
    pushd $subfolder
    mvn javadoc:javadoc
    cp -a target/site/apidocs "../${subfolder}-docs"
    popd
    pandoc "${subfolder}/Readme.md" --standalone --to html5 --css style.css -o "${subfolder}.html"
done

pandoc "Readme.md" --standalone --to html5 --css style.css -o "index.html"
