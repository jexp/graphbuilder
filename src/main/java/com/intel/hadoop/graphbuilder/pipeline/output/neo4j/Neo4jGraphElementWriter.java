/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.output.neo4j;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphElementWriter;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Vertex;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

/**
 * - duplicate edges and vertices are removed
 * - each vertex is loaded into Neo4j and is tagged with its Neo4j ID and passed to the next MR job
 *   through the temp file
 * - each edge is tagged with the Neo4j ID of its source vertex and passed to the next MR job
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.GraphElementMerge
 */
public class Neo4jGraphElementWriter extends GraphElementWriter {
    private Hashtable<Object, Long>  vertexNameToNeo4jID = new Hashtable<>();

    /**
     * Write graph elements to a Neo4j graph instance.
     * @param args
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void write(ArgumentBuilder args)
            throws IOException, InterruptedException {
        initArgs(args);

        writeVertices(args);

        writeEdges(args);
    }

    /**
     * Obtain the Neo4j-assigned ID from a Blueprints vertex
     * @param bpVertex  A Blueprints vertex.
     * @return Its Neo4j-assigned ID.
     */

    public long getVertexId(com.tinkerpop.blueprints.Vertex bpVertex){
        return ((Neo4j2Vertex)bpVertex).getRawVertex().getId();
    }

    /**
     * Writes vertices to a Neo4j graph and propagate its Neo4j-ID through an HDFs file.
     * @param args
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void writeVertices(ArgumentBuilder args) throws IOException, InterruptedException {
        initArgs(args);

        int vertexCount = 0;

        for(Map.Entry<Object, Writable> vertex: vertexSet.entrySet()) {
            // Major operation - vertex is added to Neo4j and a new ID is assigned to it
            com.tinkerpop.blueprints.Vertex  bpVertex = graph.addVertex(null);

            bpVertex.setProperty(Neo4jConfig.GB_ID_FOR_NEO4J, vertex.getKey().toString());

            long vertexId = getVertexId(bpVertex);

            Vertex tempVertex = new Vertex();

            tempVertex.configure((VertexID) vertex.getKey(), writeVertexProperties(vertexId, vertex, bpVertex));

            outValue.init(tempVertex);
            outKey.set(keyFunction.getVertexKey(tempVertex));

            context.write(outKey, outValue);

            vertexNameToNeo4jID.put(vertex.getKey(), vertexId);

            vertexCount++;
        }

        context.getCounter(vertexCounter).increment(vertexCount);
    }

    /**
     * Append the Neo4j ID of an edge's source to the edge as a property, and write the edge to an HDFS file.
     * @param args
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void writeEdges(ArgumentBuilder args)
            throws IOException, InterruptedException {
        initArgs(args);

        int edgeCount   = 0;

        for(Map.Entry<EdgeID, Writable> edge: edgeSet.entrySet()){
            Object src                  = edge.getKey().getSrc();
            Object dst                  = edge.getKey().getDst();
            String label                = edge.getKey().getLabel().toString();

            long srcNeo4jId = vertexNameToNeo4jID.get(src);

            Edge tempEdge = new Edge();

            writeEdgeProperties(srcNeo4jId, edge);

            tempEdge.configure((VertexID)  src, (VertexID)  dst, new StringType(label),
                    writeEdgeProperties(srcNeo4jId, edge));

            outValue.init(tempEdge);
            outKey.set(keyFunction.getEdgeKey(tempEdge));

            context.write(outKey, outValue);

            edgeCount++;
        }
        context.getCounter(edgeCounter).increment(edgeCount);

    }

    private PropertyMap writeVertexProperties(long vertexId, Map.Entry<Object, Writable> vertex, com.tinkerpop.blueprints.Vertex bpVertex){

        PropertyMap propertyMap = (PropertyMap) vertex.getValue();

        if(propertyMap == null){
            propertyMap = new PropertyMap();
        }

        for (Writable keyName : propertyMap.getPropertyKeys()) {
            EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

            bpVertex.setProperty(keyName.toString(), mapEntry.getBaseObject());
        }

        addNeo4jIdVertex(vertexId, propertyMap);

        return propertyMap;
    }

    private PropertyMap addNeo4jIdVertex(long vertexId,
                                         PropertyMap propertyMap) {
        propertyMap.setProperty("Neo4jID", new LongType(vertexId));
        return propertyMap;
    }

    private PropertyMap addNeo4jIdEdge(long srcNeo4jId,
                                       PropertyMap propertyMap) {
        propertyMap.setProperty(GraphElementWriter.PROPERTY_KEY_SRC_NEO4J_ID,
                new LongType(srcNeo4jId));
        return propertyMap;
    }

    private PropertyMap writeEdgeProperties(long srcNeo4jId,
                                            Map.Entry<EdgeID, Writable> edge) {

        PropertyMap propertyMap = (PropertyMap) edge.getValue();

        if(propertyMap == null){
            propertyMap = new PropertyMap();
        }

        addNeo4jIdEdge(srcNeo4jId, propertyMap);

        return propertyMap;
    }
}
