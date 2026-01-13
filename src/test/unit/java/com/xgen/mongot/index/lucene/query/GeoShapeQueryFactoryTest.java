package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.index.query.shapes.ShapeBuilder;
import com.xgen.testing.mongot.util.geo.MockGeometries;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.BoostQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GeoShapeQueryFactoryTest {
  private static final Map<GeoShapeOperator.Relation, ShapeField.QueryRelation>
      POINT_AND_LINE_RELATIONS =
          Map.of(
              GeoShapeOperator.Relation.CONTAINS,
              ShapeField.QueryRelation.CONTAINS,
              GeoShapeOperator.Relation.INTERSECTS,
              ShapeField.QueryRelation.INTERSECTS,
              GeoShapeOperator.Relation.DISJOINT,
              ShapeField.QueryRelation.DISJOINT);

  private LuceneSearchTranslation luceneTranslation;

  /** init with a "geo" field mapped as geo with shapes. */
  @Before
  public void initTranslation() {
    DocumentFieldDefinition mapping =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "geo",
                FieldDefinitionBuilder.builder()
                    .geo(GeoFieldDefinitionBuilder.builder().indexShapes(true).build())
                    .build())
            .build();
    this.luceneTranslation = LuceneSearchTranslation.mapped(mapping);
  }

  @Test
  public void testQueryIsScored() throws InvalidQueryException, IOException {
    var operator =
        OperatorBuilder.geoShape()
            .score(ScoreBuilder.valueBoost().value(4).build())
            .path("geo")
            .relation(GeoShapeOperator.Relation.INTERSECTS)
            .geometry(ShapeBuilder.geometry(MockGeometries.POINT_A))
            .build();

    var expectedInner =
        LatLonShape.newGeometryQuery(
            getShapeField(), ShapeField.QueryRelation.INTERSECTS, new Point(12, 10));
    var expected = new BoostQuery(expectedInner, 4);
    this.luceneTranslation.assertTranslatedTo(operator, expected);
  }

  @Test
  public void testPolygonTranslatesWithAllRelations() throws InvalidQueryException, IOException {
    var lucenePolygon = new Polygon(MockGeometries.POLYGON_LATS, MockGeometries.POLYGON_LONS);

    for (var entry :
        Map.of(
                GeoShapeOperator.Relation.WITHIN, ShapeField.QueryRelation.WITHIN,
                GeoShapeOperator.Relation.CONTAINS, ShapeField.QueryRelation.CONTAINS,
                GeoShapeOperator.Relation.INTERSECTS, ShapeField.QueryRelation.INTERSECTS,
                GeoShapeOperator.Relation.DISJOINT, ShapeField.QueryRelation.DISJOINT)
            .entrySet()) {
      var operator =
          OperatorBuilder.geoShape()
              .path("geo")
              .relation(entry.getKey())
              .geometry(ShapeBuilder.geometry(MockGeometries.POLYGON))
              .build();
      var expected = LatLonShape.newGeometryQuery(getShapeField(), entry.getValue(), lucenePolygon);
      this.luceneTranslation.assertTranslatedTo(operator, expected);
    }
  }

  @Test
  public void testLineAllRelations() throws InvalidQueryException, IOException {
    var luceneLine = new Line(MockGeometries.LINE_LATS, MockGeometries.LINE_LONS);
    for (var entry : POINT_AND_LINE_RELATIONS.entrySet()) {
      var operator =
          OperatorBuilder.geoShape()
              .path("geo")
              .relation(entry.getKey())
              .geometry(ShapeBuilder.geometry(MockGeometries.LINE_AB))
              .build();
      var expected = LatLonShape.newGeometryQuery(getShapeField(), entry.getValue(), luceneLine);
      this.luceneTranslation.assertTranslatedTo(operator, expected);
    }
  }

  @Test
  public void testPointAllRelations() throws InvalidQueryException, IOException {
    var lucenePoint = new Point(12, 10);
    for (var entry : POINT_AND_LINE_RELATIONS.entrySet()) {
      var operator =
          OperatorBuilder.geoShape()
              .path("geo")
              .relation(entry.getKey())
              .geometry(ShapeBuilder.geometry(MockGeometries.POINT_A))
              .build();
      var expected = LatLonShape.newGeometryQuery(getShapeField(), entry.getValue(), lucenePoint);
      this.luceneTranslation.assertTranslatedTo(operator, expected);
    }
  }

  @Test
  public void testQueryFieldNotMappedToGeoInvalid() {
    var operator =
        OperatorBuilder.geoShape()
            .path("not_geo")
            .relation(GeoShapeOperator.Relation.DISJOINT)
            .geometry(ShapeBuilder.geometry(MockGeometries.POINT_A))
            .build();
    Assert.assertThrows(
        InvalidQueryException.class, () -> LuceneSearchTranslation.get().translate(operator));
  }

  @Test
  public void testQueryFieldWithoutIndexShapesInvalid() {
    var operator =
        OperatorBuilder.geoShape()
            .path("geo_not_shape")
            .relation(GeoShapeOperator.Relation.DISJOINT)
            .geometry(ShapeBuilder.geometry(MockGeometries.POINT_A))
            .build();

    DocumentFieldDefinition mapping =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "geo_not_shape",
                FieldDefinitionBuilder.builder()
                    .geo(GeoFieldDefinitionBuilder.builder().indexShapes(false).build())
                    .build())
            .build();
    Assert.assertThrows(
        InvalidQueryException.class,
        () -> LuceneSearchTranslation.mapped(mapping).translate(operator));
  }

  private String getShapeField() {
    return "$type:geoShape/geo";
  }
}
