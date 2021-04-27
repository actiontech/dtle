package handler

import (
	_validator "github.com/go-playground/validator"
)

type CustomValidator struct {
	validator *_validator.Validate
}

func NewValidator() *CustomValidator {
	return &CustomValidator{
		validator: _validator.New(),
	}
}

func (cv *CustomValidator) Validate(i interface{}) error {
	return cv.validator.Struct(i)
}
