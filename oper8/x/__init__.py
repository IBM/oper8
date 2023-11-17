"""
The oper8.x module holds common implementations of reusable patterns built on
top of the abstractions in oper8. These are intended as reusable components that
can be share across many operator implementations.

One of the core principles of oper8 is that the schema for config is entirely up
to the user (with the _only_ exception being spec.version). In oper8.x, this is
not the case and there are many config conventions (CRD schema and backend) that
are encoded into the various utilities.
"""
