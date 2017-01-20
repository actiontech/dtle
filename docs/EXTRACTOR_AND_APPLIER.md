# Udup Extractor & Applier

As of release 0.1.0, Udup has the concept of Extractor and Applier.

These components sit in-between Input & Output, extracting and appling
events as they pass through udup:

```
                     ┌─────────────┐      ┌─────────────┐      
                     │             │      │             │   
┌───────────┐        │Extractor    │      │Applier      │        ┌───────────┐
│           │        │ - transform │      │ - batch     │        │           │
│   MySQL   │──────▶ │- filter     │─────▶│- parallel  │──────▶│  MySQL    │
│           │        │ -publish    │      │ - subscribe │        │           │
└───────────┘        │             │      │             │        └───────────┘
                     └─────────────┘      └─────────────┘    

```