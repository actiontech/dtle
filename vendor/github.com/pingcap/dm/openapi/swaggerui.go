// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package openapi

import (
	"bytes"
	"html/template"
)

const (
	defaultSwaggerHost = "https://petstore3.swagger.io"
	swaggerUITemplate  = `
	<!-- HTML for static distribution bundle build -->
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>API documentation</title>
    <link rel="stylesheet" type="text/css" href="{{ .SwaggerHost }}/swagger-ui.css" >
    <link rel="icon" type="image/png" href="{{ .SwaggerHost }}/favicon-32x32.png" sizes="32x32" />
    <link rel="icon" type="image/png" href="{{ .SwaggerHost }}/favicon-16x16.png" sizes="16x16" />
    <style>
      html
      {
        box-sizing: border-box;
        overflow: -moz-scrollbars-vertical;
        overflow-y: scroll;
      }
      *,
      *:before,
      *:after
      {
        box-sizing: inherit;
      }
      body
      {
        margin:0;
        background: #fafafa;
      }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="{{ .SwaggerHost }}/swagger-ui-bundle.js"> </script>
    <script src="{{ .SwaggerHost }}/swagger-ui-standalone-preset.js"> </script>
    <script>
    window.onload = function() {
      // Begin Swagger UI call region
      const ui = SwaggerUIBundle({
        "dom_id": "#swagger-ui",
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout",
        url: "{{ .SpecJSONPath }}",
      })
      // End Swagger UI call region
      window.ui = ui
    }
  </script>
  </body>
</html>`
)

// SwaggerConfig configures the SwaggerDoc middlewares.
type SwaggerConfig struct {
	// SpecJsonPath the url to find the spec
	SpecJSONPath string
	// SwaggerHost for the js that generates the swagger ui site, defaults to: http://petstore3.swagger.io/
	SwaggerHost string
}

// NewSwaggerConfig return swaggerConfig.
func NewSwaggerConfig(specJSONPath, swaggerHost string) *SwaggerConfig {
	if swaggerHost == "" {
		swaggerHost = defaultSwaggerHost
	}
	return &SwaggerConfig{
		SpecJSONPath: specJSONPath,
		SwaggerHost:  swaggerHost,
	}
}

// GetSwaggerHTML returns the swagger ui html.
func GetSwaggerHTML(config *SwaggerConfig) (html string, err error) {
	tmpl, err := template.New("swaggerdoc").Parse(swaggerUITemplate)
	if err != nil {
		return
	}

	buf := bytes.NewBuffer(nil)
	err = tmpl.Execute(buf, config)
	if err != nil {
		return
	}
	return buf.String(), nil
}

// GetSwaggerJSON returns the swagger json.
func GetSwaggerJSON() ([]byte, error) {
	swagger, err := GetSwagger()
	if err != nil {
		return nil, err
	}
	swaggerJSON, err := swagger.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return swaggerJSON, nil
}
