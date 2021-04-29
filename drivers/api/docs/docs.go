// GENERATED BY THE COMMAND ABOVE; DO NOT EDIT
// This file was generated by swaggo/swag

package docs

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/alecthomas/template"
	"github.com/swaggo/swag"
)

var doc = `{
    "schemes": {{ marshal .Schemes }},
    "swagger": "2.0",
    "info": {
        "description": "{{.Description}}",
        "title": "{{.Title}}",
        "contact": {},
        "version": "{{.Version}}"
    },
    "host": "{{.Host}}",
    "basePath": "{{.BasePath}}",
    "paths": {
        "/v2/database/schemas": {
            "get": {
                "description": "list schemas of datasource.",
                "tags": [
                    "datasource"
                ],
                "parameters": [
                    {
                        "type": "string",
                        "description": "mysql host",
                        "name": "mysql_host",
                        "in": "query",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "mysql port",
                        "name": "mysql_port",
                        "in": "query",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "mysql user",
                        "name": "mysql_user",
                        "in": "query",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "mysql password",
                        "name": "mysql_password",
                        "in": "query",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "mysql character set",
                        "name": "mysql_character_set",
                        "in": "query"
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.ListDatabaseSchemasRespV2"
                        }
                    }
                }
            }
        },
        "/v2/job/detail": {
            "get": {
                "description": "get job detail.",
                "tags": [
                    "job"
                ],
                "parameters": [
                    {
                        "type": "string",
                        "description": "job id",
                        "name": "job_id",
                        "in": "query",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.JobDetailRespV2"
                        }
                    }
                }
            }
        },
        "/v2/job/migration": {
            "post": {
                "description": "create or update migration job.",
                "consumes": [
                    "application/json"
                ],
                "tags": [
                    "job"
                ],
                "parameters": [
                    {
                        "description": "migration job config",
                        "name": "migration_job_config",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/models.CreateOrUpdateMysqlToMysqlJobParamV2"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.CreateOrUpdateMysqlToMysqlJobRespV2"
                        }
                    }
                }
            }
        },
        "/v2/jobs": {
            "get": {
                "description": "get job list.",
                "tags": [
                    "job"
                ],
                "parameters": [
                    {
                        "type": "string",
                        "description": "filter job type",
                        "name": "filter_job_type",
                        "in": "query"
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.JobListRespV2"
                        }
                    }
                }
            }
        },
        "/v2/log/level": {
            "post": {
                "description": "reload log level dynamically.",
                "consumes": [
                    "application/x-www-form-urlencoded"
                ],
                "tags": [
                    "log"
                ],
                "parameters": [
                    {
                        "enum": [
                            "TRACE",
                            "DEBUG",
                            "INFO",
                            "WARN",
                            "ERROR"
                        ],
                        "type": "string",
                        "description": "dtle log level",
                        "name": "dtle_log_level",
                        "in": "formData",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.UpdataLogLevelRespV2"
                        }
                    }
                }
            }
        },
        "/v2/monitor/task": {
            "get": {
                "description": "get progress of tasks within an allocation.",
                "tags": [
                    "monitor"
                ],
                "parameters": [
                    {
                        "type": "string",
                        "description": "allocation id",
                        "name": "allocation_id",
                        "in": "query",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "task name",
                        "name": "task_name",
                        "in": "query",
                        "required": true
                    },
                    {
                        "type": "string",
                        "description": "nomad_http_address is the http address of the nomad that the target dtle is running with. ignore it if you are not sure what to provide",
                        "name": "nomad_http_address",
                        "in": "query"
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.GetTaskProgressRespV2"
                        }
                    }
                }
            }
        },
        "/v2/nodes": {
            "get": {
                "description": "get node list.",
                "tags": [
                    "node"
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.NodeListRespV2"
                        }
                    }
                }
            }
        },
        "/v2/validation/job": {
            "post": {
                "description": "validate job config.",
                "consumes": [
                    "application/json"
                ],
                "tags": [
                    "validation"
                ],
                "parameters": [
                    {
                        "description": "validate job config",
                        "name": "job_config",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/models.ValidateJobReqV2"
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/models.ValidateJobRespV2"
                        }
                    }
                }
            }
        }
    },
    "definitions": {
        "models.BinlogValidation": {
            "type": "object",
            "properties": {
                "error": {
                    "description": "Error is a string version of any error that may have occured",
                    "type": "string"
                },
                "validated": {
                    "type": "boolean"
                }
            }
        },
        "models.BufferStat": {
            "type": "object",
            "properties": {
                "applier_tx_queue_size": {
                    "type": "integer"
                },
                "binlog_event_queue_size": {
                    "type": "integer"
                },
                "extractor_tx_queue_size": {
                    "type": "integer"
                },
                "send_by_size_full": {
                    "type": "integer"
                },
                "send_by_timeout": {
                    "type": "integer"
                }
            }
        },
        "models.ConnectionValidation": {
            "type": "object",
            "properties": {
                "error": {
                    "description": "Error is a string version of any error that may have occured",
                    "type": "string"
                },
                "validated": {
                    "type": "boolean"
                }
            }
        },
        "models.CreateOrUpdateMysqlToMysqlJobParamV2": {
            "type": "object",
            "required": [
                "dest_task",
                "job_name",
                "src_task"
            ],
            "properties": {
                "dest_task": {
                    "$ref": "#/definitions/models.MysqlDestTaskConfig"
                },
                "failover": {
                    "type": "boolean"
                },
                "job_id": {
                    "type": "string"
                },
                "job_name": {
                    "type": "string"
                },
                "src_task": {
                    "$ref": "#/definitions/models.MysqlSrcTaskConfig"
                }
            }
        },
        "models.CreateOrUpdateMysqlToMysqlJobRespV2": {
            "type": "object",
            "required": [
                "dest_task",
                "job_name",
                "src_task"
            ],
            "properties": {
                "dest_task": {
                    "$ref": "#/definitions/models.MysqlDestTaskConfig"
                },
                "eval_create_index": {
                    "type": "integer"
                },
                "failover": {
                    "type": "boolean"
                },
                "job_id": {
                    "type": "string"
                },
                "job_modify_index": {
                    "type": "integer"
                },
                "job_name": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                },
                "src_task": {
                    "$ref": "#/definitions/models.MysqlSrcTaskConfig"
                }
            }
        },
        "models.CurrentCoordinates": {
            "type": "object",
            "properties": {
                "file": {
                    "type": "string"
                },
                "gtid_set": {
                    "type": "string"
                },
                "position": {
                    "type": "integer"
                },
                "read_master_log_pos": {
                    "type": "integer"
                },
                "relay_master_log_file": {
                    "type": "string"
                },
                "retrieved_gtid_set": {
                    "type": "string"
                }
            }
        },
        "models.DelayCount": {
            "type": "object",
            "properties": {
                "num": {
                    "type": "integer"
                },
                "time": {
                    "type": "integer"
                }
            }
        },
        "models.GetTaskProgressRespV2": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string"
                },
                "tasks_status": {
                    "$ref": "#/definitions/models.TaskProgress"
                }
            }
        },
        "models.GtidModeValidation": {
            "type": "object",
            "properties": {
                "error": {
                    "description": "Error is a string version of any error that may have occured",
                    "type": "string"
                },
                "validated": {
                    "type": "boolean"
                }
            }
        },
        "models.JobDetailRespV2": {
            "type": "object",
            "properties": {
                "dest_task_detail": {
                    "$ref": "#/definitions/models.MysqlDestTaskDetail"
                },
                "failover": {
                    "type": "boolean"
                },
                "job_id": {
                    "type": "string"
                },
                "job_name": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                },
                "src_task_detail": {
                    "$ref": "#/definitions/models.MysqlSrcTaskDetail"
                }
            }
        },
        "models.JobListItemV2": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string"
                },
                "job_name": {
                    "type": "string"
                },
                "job_status": {
                    "type": "string"
                },
                "job_status_description": {
                    "type": "string"
                }
            }
        },
        "models.JobListRespV2": {
            "type": "object",
            "properties": {
                "jobs": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.JobListItemV2"
                    }
                },
                "message": {
                    "type": "string"
                }
            }
        },
        "models.ListDatabaseSchemasRespV2": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string"
                },
                "schemas": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.SchemaItem"
                    }
                }
            }
        },
        "models.MysqlConnectionConfig": {
            "type": "object",
            "required": [
                "mysql_host",
                "mysql_password",
                "mysql_port",
                "mysql_user"
            ],
            "properties": {
                "mysql_host": {
                    "type": "string"
                },
                "mysql_password": {
                    "type": "string"
                },
                "mysql_port": {
                    "type": "integer"
                },
                "mysql_user": {
                    "type": "string"
                }
            }
        },
        "models.MysqlDataSourceConfig": {
            "type": "object",
            "properties": {
                "table_schema": {
                    "type": "string"
                },
                "table_schema_rename": {
                    "type": "string"
                },
                "tables": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.MysqlTableConfig"
                    }
                }
            }
        },
        "models.MysqlDestTaskConfig": {
            "type": "object",
            "required": [
                "mysql_connection_config",
                "task_name"
            ],
            "properties": {
                "mysql_connection_config": {
                    "$ref": "#/definitions/models.MysqlConnectionConfig"
                },
                "node_id": {
                    "type": "string"
                },
                "parallel_workers": {
                    "type": "integer"
                },
                "task_name": {
                    "type": "string"
                }
            }
        },
        "models.MysqlDestTaskDetail": {
            "type": "object",
            "properties": {
                "allocation_id": {
                    "type": "string"
                },
                "task_config": {
                    "$ref": "#/definitions/models.MysqlDestTaskConfig"
                },
                "task_status": {
                    "$ref": "#/definitions/models.TaskStatus"
                }
            }
        },
        "models.MysqlSrcTaskConfig": {
            "type": "object",
            "required": [
                "mysql_connection_config",
                "task_name"
            ],
            "properties": {
                "chunk_size": {
                    "type": "integer"
                },
                "drop_table_if_exists": {
                    "type": "boolean"
                },
                "group_max_size": {
                    "type": "integer"
                },
                "gtid": {
                    "type": "string"
                },
                "mysql_connection_config": {
                    "$ref": "#/definitions/models.MysqlConnectionConfig"
                },
                "node_id": {
                    "type": "string"
                },
                "repl_chan_buffer_size": {
                    "type": "integer"
                },
                "replicate_do_db": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.MysqlDataSourceConfig"
                    }
                },
                "replicate_ignore_db": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.MysqlDataSourceConfig"
                    }
                },
                "skip_create_db_table": {
                    "type": "boolean"
                },
                "task_name": {
                    "type": "string"
                }
            }
        },
        "models.MysqlSrcTaskDetail": {
            "type": "object",
            "properties": {
                "allocation_id": {
                    "type": "string"
                },
                "task_config": {
                    "$ref": "#/definitions/models.MysqlSrcTaskConfig"
                },
                "task_status": {
                    "$ref": "#/definitions/models.TaskStatus"
                }
            }
        },
        "models.MysqlTableConfig": {
            "type": "object",
            "properties": {
                "column_map_from": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "table_name": {
                    "type": "string"
                },
                "table_rename": {
                    "type": "string"
                },
                "where": {
                    "type": "string"
                }
            }
        },
        "models.MysqlTaskValidationReport": {
            "type": "object",
            "properties": {
                "binlog_validation": {
                    "$ref": "#/definitions/models.BinlogValidation"
                },
                "connection_validation": {
                    "$ref": "#/definitions/models.ConnectionValidation"
                },
                "gtid_mode_validation": {
                    "$ref": "#/definitions/models.GtidModeValidation"
                },
                "privileges_validation": {
                    "$ref": "#/definitions/models.PrivilegesValidation"
                },
                "server_id_validation": {
                    "$ref": "#/definitions/models.ServerIDValidation"
                },
                "task_name": {
                    "type": "string"
                }
            }
        },
        "models.NatsMessageStatistics": {
            "type": "object",
            "properties": {
                "in_bytes": {
                    "type": "integer"
                },
                "in_messages": {
                    "type": "integer"
                },
                "out_bytes": {
                    "type": "integer"
                },
                "out_messages": {
                    "type": "integer"
                },
                "reconnects": {
                    "type": "integer"
                }
            }
        },
        "models.NodeListItemV2": {
            "type": "object",
            "properties": {
                "datacenter": {
                    "type": "string"
                },
                "node_address": {
                    "type": "string"
                },
                "node_id": {
                    "type": "string"
                },
                "node_name": {
                    "type": "string"
                },
                "node_status": {
                    "type": "string"
                },
                "node_status_description": {
                    "type": "string"
                }
            }
        },
        "models.NodeListRespV2": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string"
                },
                "nodes": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.NodeListItemV2"
                    }
                }
            }
        },
        "models.PrivilegesValidation": {
            "type": "object",
            "properties": {
                "error": {
                    "description": "Error is a string version of any error that may have occured",
                    "type": "string"
                },
                "validated": {
                    "type": "boolean"
                }
            }
        },
        "models.SchemaItem": {
            "type": "object",
            "properties": {
                "schema_name": {
                    "type": "string"
                },
                "tables": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.TableItem"
                    }
                }
            }
        },
        "models.ServerIDValidation": {
            "type": "object",
            "properties": {
                "error": {
                    "description": "Error is a string version of any error that may have occured",
                    "type": "string"
                },
                "validated": {
                    "type": "boolean"
                }
            }
        },
        "models.TableItem": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string"
                }
            }
        },
        "models.TaskEvent": {
            "type": "object",
            "properties": {
                "event_type": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                },
                "setup_error": {
                    "type": "string"
                },
                "time": {
                    "type": "string"
                }
            }
        },
        "models.TaskProgress": {
            "type": "object",
            "properties": {
                "ETA": {
                    "type": "string"
                },
                "backlog": {
                    "type": "string"
                },
                "buffer_status": {
                    "$ref": "#/definitions/models.BufferStat"
                },
                "current_coordinates": {
                    "$ref": "#/definitions/models.CurrentCoordinates"
                },
                "delay_count": {
                    "$ref": "#/definitions/models.DelayCount"
                },
                "exec_master_row_count": {
                    "type": "integer"
                },
                "exec_master_tx_count": {
                    "type": "integer"
                },
                "nats_message_status": {
                    "$ref": "#/definitions/models.NatsMessageStatistics"
                },
                "progress_PCT": {
                    "type": "string"
                },
                "read_master_row_count": {
                    "type": "integer"
                },
                "read_master_tx_count": {
                    "type": "integer"
                },
                "stage": {
                    "type": "string"
                },
                "throughput_status": {
                    "$ref": "#/definitions/models.ThroughputStat"
                },
                "timestamp": {
                    "type": "integer"
                }
            }
        },
        "models.TaskStatus": {
            "type": "object",
            "properties": {
                "finished_at": {
                    "type": "string"
                },
                "started_at": {
                    "type": "string"
                },
                "status": {
                    "type": "string"
                },
                "task_events": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.TaskEvent"
                    }
                }
            }
        },
        "models.ThroughputStat": {
            "type": "object",
            "properties": {
                "num": {
                    "type": "integer"
                },
                "time": {
                    "type": "integer"
                }
            }
        },
        "models.UpdataLogLevelRespV2": {
            "type": "object",
            "properties": {
                "dtle_log_level": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                }
            }
        },
        "models.ValidateJobReqV2": {
            "type": "object",
            "required": [
                "dest_task",
                "job_name",
                "src_task"
            ],
            "properties": {
                "dest_task": {
                    "$ref": "#/definitions/models.MysqlDestTaskConfig"
                },
                "failover": {
                    "type": "boolean"
                },
                "job_id": {
                    "type": "string"
                },
                "job_name": {
                    "type": "string"
                },
                "src_task": {
                    "$ref": "#/definitions/models.MysqlSrcTaskConfig"
                }
            }
        },
        "models.ValidateJobRespV2": {
            "type": "object",
            "properties": {
                "driver_config_validated": {
                    "description": "DriverConfigValidated indicates whether the agent validated the driver",
                    "type": "boolean"
                },
                "job_validation_error": {
                    "type": "string"
                },
                "job_validation_warning": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                },
                "mysql_task_validation_report": {
                    "description": "config",
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/models.MysqlTaskValidationReport"
                    }
                }
            }
        }
    }
}`

type swaggerInfo struct {
	Version     string
	Host        string
	BasePath    string
	Schemes     []string
	Title       string
	Description string
}

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = swaggerInfo{
	Version:     "",
	Host:        "",
	BasePath:    "",
	Schemes:     []string{},
	Title:       "",
	Description: "",
}

type s struct{}

func (s *s) ReadDoc() string {
	sInfo := SwaggerInfo
	sInfo.Description = strings.Replace(sInfo.Description, "\n", "\\n", -1)

	t, err := template.New("swagger_info").Funcs(template.FuncMap{
		"marshal": func(v interface{}) string {
			a, _ := json.Marshal(v)
			return string(a)
		},
	}).Parse(doc)
	if err != nil {
		return doc
	}

	var tpl bytes.Buffer
	if err := t.Execute(&tpl, sInfo); err != nil {
		return doc
	}

	return tpl.String()
}

func init() {
	swag.Register(swag.Name, &s{})
}
