package com.xgen.mongot.index.lucene;

import org.apache.lucene.facet.DrillDownQuery;

public record LuceneDrillSideways(
    MongotDrillSideways drillSideways, DrillDownQuery drillDownQuery) {}
