package com.xgen.mongot.index.lucene.query;

import com.mongodb.client.model.geojson.Point;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.geo.InvalidGeoPosition;
import com.xgen.mongot.index.lucene.geo.LuceneGeoTranslator;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.points.GeoPoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import java.util.Optional;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.search.Query;

class GeoNearQueryFactory {

  static CheckedBiFunction<FieldPath, Optional<FieldPath>, Query, InvalidQueryException> geoQuery(
      GeoPoint origin, double pivotMeters, SearchQueryFactoryContext queryFactoryContext)
      throws InvalidQueryException {

    Point geoPoint = origin.value();
    org.apache.lucene.geo.Point point =
        InvalidQueryException.wrapIfThrows(
            () -> LuceneGeoTranslator.point(geoPoint), InvalidGeoPosition.class);

    return (path, embeddedRoot) -> {
      InvalidQueryException.validate(
          queryFactoryContext.getQueryTimeMappingChecks().indexedAsGeoPoint(path, embeddedRoot),
          "near with a geo point origin requires path '%s' to be indexed as 'geo'",
          path);

      return LatLonPoint.newDistanceFeatureQuery(
          FieldName.TypeField.GEO_POINT.getLuceneFieldName(path, Optional.empty()),
          1.0f,
          point.getLat(),
          point.getLon(),
          pivotMeters);
    };
  }
}
