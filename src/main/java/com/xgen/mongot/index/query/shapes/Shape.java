package com.xgen.mongot.index.query.shapes;

import com.xgen.mongot.util.bson.parser.DocumentEncodable;

/** Interface for shapes other than GeoJson. Used in query time. */
public sealed interface Shape extends DocumentEncodable permits Box, Circle, GeometryShape {}
