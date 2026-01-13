package com.xgen.mongot.server.command;

import org.bson.BsonDocument;

public record ParsedCommand(String name, BsonDocument body) {}
