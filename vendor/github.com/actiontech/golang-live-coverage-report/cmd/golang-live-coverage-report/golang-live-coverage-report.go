package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
)

var flagPackgeName = flag.String("bootstrap-package-name", "", "package name of generated bootstrap golang code")
var flagBootstrapOutputFile = flag.String("bootstrap-outfile", "", "target file path of generated bootstrap golang code")
var flagPreCompile = flag.Bool("pre-build", false, "mode: before build, it will preserve raw code, and generate cover code")
var flagPostCompile = flag.Bool("post-build", false, "mode: after build, it will rollback replaced code, and clean the generated cover code")
var flagPreCompileRawCodeOutputDirectory = flag.String("raw-code-build-dir", "", "before compilation, raw codes will be moved to this directory. Then you can deploy the raw source code from here in compilation")
var flagRawCodeDeployDirectory = flag.String("raw-code-deploy-dir", "", "the raw code should be accessible in production environment, you may specific relative deploy directory by -raw-code-deploy-dir")
var flagGo = flag.String("go", "go", "specific go binary path")

func exitIfErr(err error) {
	if nil != err {
		log.Fatalf("ERROR: %v\n", err)
	}
}

func checkFlag() {
	if *flagPreCompile {
		if "" == *flagPreCompileRawCodeOutputDirectory {
			exitIfErr(errors.New("-raw-code-build-dir is required"))
		}
		if "" == *flagRawCodeDeployDirectory {
			exitIfErr(errors.New("-raw-code-deploy-dir is required"))
		}
		if "" == *flagPackgeName {
			exitIfErr(errors.New("-bootstrap-package-name is required"))
		}
		if "" == *flagBootstrapOutputFile {
			exitIfErr(errors.New("-bootstrap-outfile is required"))
		}
	}
	if *flagPostCompile {
		if "" == *flagPreCompileRawCodeOutputDirectory {
			exitIfErr(errors.New("-raw-code-build-dir is required"))
		}
		if "" == *flagBootstrapOutputFile {
			exitIfErr(errors.New("-bootstrap-outfile is required"))
		}
	}
}

func main() {
	flag.Parse()
	checkFlag()

	files, err := getFilesFromArgs()
	exitIfErr(err)

	if *flagPreCompile {
		err := checkRawCode(files)
		exitIfErr(err)

		err = preserveRawCode(files, *flagPreCompileRawCodeOutputDirectory)
		exitIfErr(err)

		files = makeGoCoverCommands(files, *flagPreCompileRawCodeOutputDirectory, *flagGo)
		err = runGoCoverCommands(files)
		exitIfErr(err)

		golangCode, err := makeBootstrapGolangCode(*flagPackgeName, *flagRawCodeDeployDirectory, *flagBootstrapOutputFile, files)
		exitIfErr(err)

		err = ioutil.WriteFile(*flagBootstrapOutputFile, []byte(golangCode), 0666)
		exitIfErr(err)

		log.Printf(`Now, you can build your project:
1. The target codes are replaced by adding monitors inside (see "go tool cover")
2. A bootstrap go file is generated at %v, it will enable the coverage reports, remember build your project with it
3. The raw codes are moved to %v, remember deploy it to %v in your building
4. Remember run "-post-compile" after your building, it will recover the changes above

`,
			*flagBootstrapOutputFile,
			*flagPreCompileRawCodeOutputDirectory,
			*flagRawCodeDeployDirectory,
		)
		return
	}

	if *flagPostCompile {
		os.Remove(*flagBootstrapOutputFile)
		recoverFiles(files, *flagPreCompileRawCodeOutputDirectory)
		return
	}

	exitIfErr(errors.New("unsupported mode. Mode could be '-pre-build' or '-post-compile'"))
}

func recoverFiles(files tFiles, outDir string) {
	for _, file := range files {
		os.Rename(filepath.Join(outDir, file.RelPath), filepath.Join(file.RootPath, file.RelPath))
	}
	os.RemoveAll(outDir)
}

func checkRawCode(files tFiles) error {
	for _, file := range files {
		path := filepath.Join(file.RootPath, file.RelPath)
		bs, err := ioutil.ReadFile(path)
		if nil != err {
			return err
		}
		if strings.HasPrefix(string(bs), "//line ") {
			return fmt.Errorf("check source code error: %v is already replaced with 'go tool cover', run -post-compile first", path)
		}
	}
	return nil
}

func runGoCoverCommands(files tFiles) error {
	for _, file := range files {
		out, err := file.GoCoverCmd.CombinedOutput()
		if nil != err {
			return fmt.Errorf("'go tool cover' error: %v", string(out))
		}
	}
	return nil
}

func preserveRawCode(files tFiles, outDir string) error {
	for _, file := range files {
		relDir := filepath.Dir(file.RelPath)

		//mkdir -p
		if !("." == relDir || string(os.PathSeparator) == relDir) {
			if err := os.MkdirAll(filepath.Join(outDir, relDir), 0755); nil != err {
				return err
			}
		}

		if err := os.Rename(filepath.Join(file.RootPath, file.RelPath), filepath.Join(outDir, file.RelPath)); nil != err {
			return err
		}
	}
	return nil
}

type tFile struct {
	RootPath   string
	RelPath    string

	ImportPath string
	ImportVar  string

	GoCoverCmd *exec.Cmd
	VarName    string //variable name in `go tool cover` generated code, like 'GoCover_??'
}

type tFiles []*tFile

func newFile(absPath, rootPath, importPath string) (*tFile, error) {
	relPath, err := filepath.Rel(rootPath, absPath)
	if nil != err {
		return nil, err
	}
	return &tFile{
		RelPath:    relPath,
		ImportPath: importPath,
		RootPath:   rootPath,
	}, nil
}

func makeBootstrapGolangCode(packageName, rawCodeDeployDir, bootstrapOutFile string, files tFiles) (string, error) {
	bootstrapImportPath, err := getBootstrapImportPath(bootstrapOutFile)
	if nil != err {
		return "", err
	}

	if "" == packageName {
		packageName = filepath.Base(bootstrapImportPath)
	}

	importLines := make([]string, 0)
	{
		imports := map[string]string{}
		importSeq := 0

		for _, file := range files {
			importPath := file.ImportPath
			if bootstrapImportPath == importPath {
				continue
			}
			importVar, found := imports[importPath]
			if !found {
				importSeq ++
				imports[importPath] = fmt.Sprintf("imp%v", importSeq)
				importVar = imports[importPath]
				importLines = append(importLines, fmt.Sprintf(`%v "%v"`, importVar, importPath))
			}
			file.ImportVar = importVar
		}
	}

	buf := bytes.NewBufferString("")
	err = bootstrapGolangCodeTpl.Execute(buf, tBootstrapGolangCodeTpl{
		PackageName:       packageName,
		Files:             files,
		RawCodeDeployDir:  rawCodeDeployDir,
		PackageImportPath: bootstrapImportPath,
		ImportLines:       importLines,
	})
	if nil != err {
		return "", err
	}
	return buf.String(), nil
}

func getBootstrapImportPath(bootstrapOutFile string) (string, error) {
	bootstrapOutFileAbs, err := filepath.Abs(bootstrapOutFile)
	if nil != err {
		return "", err
	}
	goListJson, err := goList(filepath.Dir(bootstrapOutFileAbs))
	if nil != err {
		return "", err
	}
	return goListJson.ImportPath, nil
}

type tBootstrapGolangCodeTpl struct {
	PackageName       string
	Files             tFiles
	RawCodeDeployDir  string
	PackageImportPath string
	ImportLines       []string
}

var bootstrapGolangCodeTpl = template.Must(
	template.New("golang").
		Funcs(template.FuncMap{
			"join": filepath.Join,
		}).
		Parse(bootstrapGolangCodeTplText))

var bootstrapGolangCodeTplText = `package {{.PackageName}}

import (
	report "github.com/actiontech/golang-live-coverage-report/pkg"
	{{range $i, $f := .ImportLines}}
	{{$f}}
	{{end}}
)

func init() {
	//generated
	{{range $i, $f := .Files}}
	report.RegisterCover(
		"{{join $.RawCodeDeployDir $f.RelPath}}", {{if eq "" $f.ImportVar}}
		{{$f.VarName}}.Count[:], {{$f.VarName}}.Pos[:], {{$f.VarName}}.NumStmt[:])
		{{else}}
		{{$f.ImportVar}}.{{$f.VarName}}.Count[:], {{$f.ImportVar}}.{{$f.VarName}}.Pos[:], {{$f.ImportVar}}.{{$f.VarName}}.NumStmt[:])
		{{end}}
	{{end}}
}

`

func makeGoCoverCommands(files tFiles, rawCodeCompileDir, goBin string) tFiles {
	for _, file := range files {
		fileName := filepath.Base(file.RelPath)
		file.VarName = fmt.Sprintf("GoCover_%v", strings.Replace(fileName, ".", "_", -1))
		file.GoCoverCmd = exec.Command(goBin,
			"tool", "cover",
			"-mode", "set",
			"-var", file.VarName,
			"-o", filepath.Join(file.RootPath, file.RelPath),
			filepath.Join(rawCodeCompileDir, file.RelPath))
	}
	return files
}

func getFilesFromArgs() (tFiles, error) {
	ret := make(tFiles, 0)
	for _, filePath := range flag.Args() {
		files, err := getFilesByFilePath(filePath)
		if nil != err {
			return nil, err
		}
		ret = append(ret, files...)
	}
	return ret, nil
}

type GoListJson struct {
	ImportPath string
	GoFiles    []string
	Root       string
}

const singleFileImportPath = "command-line-arguments"

func getFilesByFilePath(path string) (tFiles, error) {
	var err error
	path, err = filepath.Abs(path)
	if nil != err {
		return nil, err
	}

	goListJson, err := goList(path)
	if nil != err {
		return nil, err
	}
	if singleFileImportPath == goListJson.ImportPath {
		parentGoListJson, err := goList(filepath.Dir(path))
		if nil != err {
			return nil, err
		}
		f, err := newFile(path, goListJson.Root, parentGoListJson.ImportPath)
		if nil != err {
			return nil, err
		}
		return tFiles{f}, nil
	} else {
		ret := make(tFiles, 0)
		for _, fileName := range goListJson.GoFiles {
			f, err := newFile(filepath.Join(path, fileName), goListJson.Root, goListJson.ImportPath)
			if nil != err {
				return nil, err
			}
			ret = append(ret, f)
		}
		return ret, nil
	}
}

func goList(file string) (*GoListJson, error) {
	cmd := exec.Command("go", "list", "-json", "-mod=vendor", file)
	out, err := cmd.CombinedOutput()
	if nil != err {
		return nil, fmt.Errorf("go list %v error: %v", file, string(out))
	}
	goListJson := GoListJson{}
	if err := json.Unmarshal(out, &goListJson); nil != err {
		return nil, fmt.Errorf("unmarshal go list error: %v", err)
	}
	return &goListJson, nil
}
