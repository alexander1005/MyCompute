package com.boraydata.env.factoy;

import com.boraydata.cluster.ClusterClient;
import com.boraydata.env.ExecutionEnvironment;

import java.net.URL;
import java.util.List;

public class ContextEnvironmentFactory implements ExecutionEnvironmentFactory {


    private final ClusterClient client;

    private final List<URL> jarFilesToAttach;

    private final List<URL> classpathsToAttach;

    private final ClassLoader userCodeClassLoader;

    private final int defaultParallelism;

    private final boolean isDetached;



    public ContextEnvironmentFactory(ClusterClient client, List<URL> jarFilesToAttach,
                                     List<URL> classpathsToAttach, ClassLoader userCodeClassLoader, int defaultParallelism,
                                     boolean isDetached) {
        this.client = client;
        this.jarFilesToAttach = jarFilesToAttach;
        this.classpathsToAttach = classpathsToAttach;
        this.userCodeClassLoader = userCodeClassLoader;
        this.defaultParallelism = defaultParallelism;
        this.isDetached = isDetached;
    }


    public ExecutionEnvironment createExecutionEnvironment() {



        return null;
    }
}
