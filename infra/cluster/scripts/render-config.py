#!/usr/bin/env python3
"""Render a ${var} template: render-config.py TEMPLATE key=value ... > out.properties

Same ${var} syntax as Terraform templatefile(), so one template serves both the
local spike and the AWS nodes."""
import sys
from string import Template

tmpl = Template(open(sys.argv[1]).read())
print(tmpl.substitute(dict(a.split("=", 1) for a in sys.argv[2:])), end="")
