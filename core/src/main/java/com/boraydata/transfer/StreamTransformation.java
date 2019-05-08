package com.boraydata.transfer;

import com.boraydata.env.Model;
import com.boraydata.out.Output;

import java.util.Collection;
import java.util.UUID;

public abstract class StreamTransformation<T> {


    // This is used to assign a unique ID to every StreamTransformation
    protected static Integer idCounter = 0;

    public static int getNewNodeId() {
        idCounter++;
        return idCounter;
    }


    public Model model;

    protected final int id;

    protected String name;

    // This is used to handle MissingTypeInfo. As long as the outputType has not been queried
    // it can still be changed using setOutputType(). Afterwards an exception is thrown when
    // trying to change the output type.
    protected boolean typeUsed;

    protected int parallelism;

    /**
     * The maximum parallelism for this stream transformation. It defines the upper limit for
     * dynamic scaling and the number of key groups used for partitioned state.
     */
    private int maxParallelism = -1;


    /**
     * User-specified ID for this transformation. This is used to assign the
     * same operator ID across job restarts. There is also the automatically
     * generated {@link #id}, which is assigned from a static counter. That
     * field is independent from this.
     */
    private String uid;

    private String userProvidedNodeHash;

    protected long bufferTimeout = -1;

    private String slotSharingGroup;


    /**
     * Creates a new {@code StreamTransformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log

     * @param parallelism The parallelism of this {@code StreamTransformation}
     */
    public StreamTransformation(String name,  int parallelism) {
        this.id = getNewNodeId();
        this.name = name;
        this.parallelism = parallelism;
        this.slotSharingGroup = null;
    }

    /**
     * Returns the unique ID of this {@code StreamTransformation}.
     */
    public int getId() {
        return id;
    }

    /**
     * Changes the name of this {@code StreamTransformation}.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the name of this {@code StreamTransformation}.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the parallelism of this {@code StreamTransformation}.
     */
    public int getParallelism() {
        return parallelism;
    }


    /**
     * Gets the maximum parallelism for this stream transformation.
     *
     * @return Maximum parallelism of this transformation.
     */
    public int getMaxParallelism() {
        return maxParallelism;
    }

    public void setUidHash(String uidHash) {

        this.userProvidedNodeHash = uidHash;
    }

    /**
     * Gets the user provided hash.
     *
     * @return The user provided hash.
     */
    public String getUserProvidedNodeHash() {
        return userProvidedNodeHash;
    }

    /**
     * Sets an ID for this {@link StreamTransformation}. This is will later be hashed to a uidHash which is then used to
     * create the JobVertexID (that is shown in logs and the web ui).
     *
     * <p>The specified ID is used to assign the same operator ID across job
     * submissions (for example when starting a job from a savepoint).
     *
     * <p><strong>Important</strong>: this ID needs to be unique per
     * transformation and job. Otherwise, job submission will fail.
     *
     * @param uid The unique user-specified ID of this transformation.
     */
    public void setUid(String uid) {
        this.uid = uid;
    }

    /**
     * Returns the user-specified ID of this transformation.
     *
     * @return The unique user-specified ID of this transformation.
     */
    public String getUid() {
        return uid;
    }

    /**
     * Returns the slot sharing group of this transformation.
     *
     * @see #setSlotSharingGroup(String)
     */
    public String getSlotSharingGroup() {
        return slotSharingGroup;
    }

    /**
     * Sets the slot sharing group of this transformation. Parallel instances of operations that
     * are in the same slot sharing group will be co-located in the same TaskManager slot, if
     * possible.
     *
     * <p>Initially, an operation is in the default slot sharing group. This can be explicitly
     * set using {@code setSlotSharingGroup("default")}.
     *
     * @param slotSharingGroup The slot sharing group name.
     */
    public void setSlotSharingGroup(String slotSharingGroup) {
        this.slotSharingGroup = slotSharingGroup;
    }

    /**
     * Tries to fill in the type information. Type information can be filled in
     * later when the program uses a type hint. This method checks whether the
     * type information has ever been accessed before and does not allow
     * modifications if the type was accessed already. This ensures consistency
     * by making sure different parts of the operation do not assume different
     * type information.
     **
     * @throws IllegalStateException Thrown, if the type information has been accessed before.
     */



    public void setBufferTimeout(long bufferTimeout) {
        this.bufferTimeout = bufferTimeout;
    }

    /**
     * Returns the buffer timeout of this {@code StreamTransformation}.
     *
     * @see #setBufferTimeout(long)
     */
    public long getBufferTimeout() {
        return bufferTimeout;
    }

    /**
     * Returns all transitive predecessor {@code StreamTransformation}s of this {@code StreamTransformation}. This
     * is, for example, used when determining whether a feedback edge of an iteration
     * actually has the iteration head as a predecessor.
     *
     * @return The list of transitive predecessors.
     */
    public abstract Collection<StreamTransformation<?>> getTransitivePredecessors();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", parallelism=" + parallelism +
                '}';
    }


    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + name.hashCode();
        result = 31 * result + parallelism;
        result = 31 * result + (int) (bufferTimeout ^ (bufferTimeout >>> 32));
        return result;
    }

    public abstract Output getOut();

    public abstract void setParallelism(int parallelism);

    public void setMaxParallelism(int globalMaxParallelismFromConfig) {
        this.maxParallelism = globalMaxParallelismFromConfig;
    }
}
