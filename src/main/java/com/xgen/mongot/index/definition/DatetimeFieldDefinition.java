package com.xgen.mongot.index.definition;

/** An interface for fields with date values to report their date options. */
public sealed interface DatetimeFieldDefinition
    permits DateFieldDefinition, DateFacetFieldDefinition {}
