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
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

/**
 * Write edges with Neo4j vertex ID of the target vertex to HDFS.
 * <p>
 * This class gathers each vertex with the edges that point to that vertex,
 * that is, those edges for which the vertex is the destination. Because the
 * edges were tagged with the Neo4j IDs of their sources in the previous Map
 * Reduce job and each vertex is tagged with its Neo4j ID,
 * we now know the Neo4j ID of the source and destination of the edges and
 * can add them to Neo4j.
 * </p>
 */

public class IntermediateEdgeWriterReducer extends Reducer<IntWritable,
        SerializedGraphElement, IntWritable, SerializedGraphElement> {
    private static final Logger LOG = Logger.getLogger(
                IntermediateEdgeWriterReducer.class);

    private Hashtable<Object, Long> vertexNameToNeo4jID;

    private IntermediateEdgeWriterReducerCallback intermediateEdgeWriterReducerCallback;

    private final KeyFunction keyFunction = new SourceVertexKeyFunction();
    private IntWritable            outKey;
    private SerializedGraphElement outValue;
    private Class                  outClass;

    private static enum Counters {
        EDGE_PROPERTIES_WRITTEN,
        NUM_EDGES
    }

    /*
     * Creates the Neo4j graph for saving edges and removes the static open
     * method from setup so it can be mocked-up.
     *
     * @return {@code Neo4j2Graph}  For saving edges.
     * @throws IOException
     */
    private Neo4j2Graph getNeo4j2GraphInstance (Context context) throws
            IOException {
        BaseConfiguration neo4jConfig = new BaseConfiguration();
        return (Neo4j2Graph) GraphDatabaseConnector.open("neo4j",
                neo4jConfig,
                context.getConfiguration());
    }

    /**
     * Sets up the Neo4j connection.
     *
     * @param {@code context}  The reducer context provided by Hadoop.
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        this.vertexNameToNeo4jID = new Hashtable<Object, Long>();

        intermediateEdgeWriterReducerCallback = new IntermediateEdgeWriterReducerCallback();

        outClass = context.getMapOutputValueClass();
        outKey   = new IntWritable();

        try {
            outValue   = (SerializedGraphElement) outClass.newInstance();
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR, "GRAPHBUILDER_ERROR: Cannot " +
                    "instantiate new reducer output value ( " + outClass
                    .getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR, "GRAPHBUILDER_ERROR: Illegal " +
                    "access exception when instantiating reducer output value" +
                    " ( " + outClass.getName() + ")", LOG, e);
        }

    }

    /**
     * This routine is called by Hadoop to write edges to an
     * intermediate HDFS  location
     * <p>
     * We assume that the edges and vertices have been gathered so that every
     * edge shares the reducer of its destination vertex,
     * and that every edge has previously been assigned the Neo4j ID of its
     * source vertex.
     * </p>
     * <p>
     * Neo4j IDs are propagated from the destination vertices to each edge
     * </p>
     * @param {@code key}      A map reduce key; a hash of a vertex ID.
     * @param {@code values}   Either a vertex with that hashed vertex ID,
     *                         or an edge with said vertex as its destination.
     * @param {@code context}  A reducer context provided by Hadoop.
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<SerializedGraphElement>
            values, Context context)
            throws IOException, InterruptedException {

        Hashtable<EdgeID, Writable> edgePropertyTable  = new Hashtable();

        for(SerializedGraphElement graphElement: values){
            /*
             * This is calling IntermediateEdgeWriterReducerCallback which is an
             * implementation of GraphElementTypeCallback to add all the
             * edges and vertices into  the edgePropertyTable and
             * vertexNameToNeo4jID hashmaps
             */
            graphElement.graphElement().typeCallback(
                    intermediateEdgeWriterReducerCallback,
                    ArgumentBuilder.newArguments()
                            .with("edgePropertyTable", edgePropertyTable)
                            .with("vertexNameToNeo4jID",
                    vertexNameToNeo4jID));
        }

        int edgeCount   = 0;
        StringType edgeLabel = new StringType();

        // Output edge records

        for (Map.Entry<EdgeID, Writable> edgeMapEntry :
                    edgePropertyTable.entrySet()) {

            VertexID<StringType> srcVertexId = (VertexID<StringType>)
                    edgeMapEntry.getKey().getSrc();
            VertexID<StringType> tgtVertexId = (VertexID<StringType>)
                    edgeMapEntry.getKey().getDst();
            edgeLabel.set(edgeMapEntry.getKey().getLabel().toString());
            PropertyMap propertyMap = (PropertyMap) edgeMapEntry.getValue();

            Edge tempEdge = new Edge();
            tempEdge.configure(srcVertexId, tgtVertexId, edgeLabel, propertyMap);

            // Add the Neo4j ID of the target vertex

            long dstNeo4jId
                    = vertexNameToNeo4jID.get(edgeMapEntry.getKey().getDst());
            tempEdge.setProperty("tgtNeo4jID", new LongType(dstNeo4jId));

            outValue.init(tempEdge);
            outKey.set(keyFunction.getEdgeKey(tempEdge));

            context.write(outKey, outValue);

            tempEdge = null;

            edgeCount++;

         }  // End of for loop on edges

        context.getCounter(Counters.NUM_EDGES).increment(edgeCount);
    }   // End of reduce

    public  Enum getEdgeCounter(){
        return Counters.NUM_EDGES;
    }

    public Enum getEdgePropertiesCounter(){
        return Counters.EDGE_PROPERTIES_WRITTEN;
    }
}
