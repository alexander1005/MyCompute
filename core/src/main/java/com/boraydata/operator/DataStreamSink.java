package com.boraydata.operator;

import com.boraydata.env.ExecutionEnvironment;
import com.boraydata.transfer.SinkTransformation;

public class DataStreamSink<T> extends DataStream<T> {
    /**
     * Create a new {@link DataStream} in the given execution environment with
     * partitioning set to forward by default.
     *
     * @param environment    The StreamExecutionEnvironment
     * @param transformation
     */
    public final SinkTransformation<T> transformation;

    public DataStreamSink(DataStream<T> inputStream, StreamSink<T> operator,ExecutionEnvironment environment) {
        super(environment,new SinkTransformation<T>(inputStream.getTransformation(), "Unnamed", operator, 1));
        this.transformation = (SinkTransformation<T>) super.transformation;
    }

    /**
     * Returns the transformation that contains the actual sink operator of this sink.
     */
    public SinkTransformation getTransformation() {
        return transformation;
    }

    /**
     * Sets the name of this sink. This name is
     * used by the visualization and logging during runtime.
     *
     * @return The named sink.
     */
    public DataStreamSink<T> name(String name) {
        transformation.setName(name);
        return this;
    }

    /**
     * Sets an ID for this operator.
     *
     * <p>The specified ID is used to assign the same operator ID across job
     * submissions (for example when starting a job from a savepoint).
     *
     * <p><strong>Important</strong>: this ID needs to be unique per
     * transformation and job. Otherwise, job submission will fail.
     *
     * @param uid The unique user-specified ID of this transformation.
     * @return The operator with the specified ID.
     */
    public DataStreamSink<T> uid(String uid) {
        transformation.setUid(uid);
        return this;
    }

    /**
     * Sets an user provided hash for this operator. This will be used AS IS the create the JobVertexID.
     *
     * <p>The user provided hash is an alternative to the generated hashes, that is considered when identifying an
     * operator through the default hash mechanics fails (e.g. because of changes between Flink versions).
     *
     * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting. The provided hash
     * needs to be unique per transformation and job. Otherwise, job submission will fail. Furthermore, you cannot
     * assign user-specified hash to intermediate nodes in an operator chain and trying so will let your job fail.
     *
     * <p>A use case for this is in migration between Flink versions or changing the jobs in a way that changes the
     * automatically generated hashes. In this case, providing the previous hashes directly through this method (e.g.
     * obtained from old logs) can help to reestablish a lost mapping from states to their target operator.
     *
     * @param uidHash The user provided hash for this operator. This will become the JobVertexID, which is shown in the
     *                 logs and web ui.
     * @return The operator with the user provided hash.
     */
    public DataStreamSink<T> setUidHash(String uidHash) {
        transformation.setUidHash(uidHash);
        return this;
    }

    /**
     * Sets the parallelism for this sink. The degree must be higher than zero.
     *
     * @param parallelism The parallelism for this sink.
     * @return The sink with set parallelism.
     */
    public DataStreamSink<T> setParallelism(int parallelism) {
        transformation.setParallelism(parallelism);
        return this;
    }

}
