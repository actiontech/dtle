# golang-live-coverage-report

Golang coverage report was official supported by `go test -coverprofile`.

This tool makes it available for **integration test** of long-run project, e.g. a web server.

Most code is copied from golang `cmd/cover` and `testing` pakcage.

**Support golang 1.11+**. 

# Demo
1. Run the following script

    ```
    # prepare temp directory
    mkdir /tmp/gopath
    cd /tmp/gopath
    
    # go get
    GOPATH=/tmp/gopath go get github.com/ikarishinjieva/golang-live-coverage-report/cmd/golang-live-coverage-report
    
    # build demo
    cd /tmp/gopath/src/github.com/ikarishinjieva/golang-live-coverage-report/examples/
    GOPATH=/tmp/gopath PATH=$GOPATH/bin:$PATH make
    
    # start demo server
    ./demo 
    ```  

2. open a browser with http://localhost:8080/report, you will see a report like: 

![Coverage Report 1](https://github.com/ikarishinjieva/golang-live-coverage-report/blob/master/web/cover1.png)

3. open http://localhost:8080/run_once, to run the target function once

4. open http://localhost:8080/report again, you will see the report with some code line already covered

![Coverage Report 2](https://github.com/ikarishinjieva/golang-live-coverage-report/blob/master/web/cover2.png)

# How to use

The brief steps of this tool are: 

1. Before building your project, run `golang-live-coverage-report -pre-build ... {files-included-in-the-report}`:

    1.1 This tool will copy the files to temporary directory (`-raw-code-build-dir`)
    
    1.2 This tool will inject some codes into the files
    
    1.3 This tool will generate a bootstrap golang file (`-bootstrap-outfile`), the package name could be specified by `-bootstrap-package-name`
    
2. Build your project:

    2.1 Add a HTTP handler using `GenerateHtmlReport()` in your code, example is [here](https://github.com/ikarishinjieva/golang-live-coverage-report/blob/master/examples/demo.go)
    
    2.2 Compile your project with the bootstrap file
    
    2.3 Remember to exclude the temporary directory in step 1.1 when compiling
    
    2.4 Copy the temporary directory to `{your building directory}/{path specified by '-raw-code-deploy-dir'}`
    
3. After building your project, run `golang-live-coverage-report -post-build ... {files-included-in-the-report}` to rollback the changes:

    3.1 Remove the bootstrap file in step 1.3
    
    3.2 Rollback the injected files, with files in temporary directory in step 1.1
    
    3.3 Remove the temporary directory in step 1.1
    
4. The entire process example is [here](https://github.com/ikarishinjieva/golang-live-coverage-report/blob/master/examples/Makefile)