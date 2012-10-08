package org.elasticsearch.shape.dataset;

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.vividsolutions.jts.geom.GeometryFactory;

// TODO: Remove this class once we have access to it in ElasticSearch
public interface GeoShapeConstants {

    JtsSpatialContext SPATIAL_CONTEXT = new JtsSpatialContext(true);

    GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
}
