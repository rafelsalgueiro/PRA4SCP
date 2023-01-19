package info.trekto.jos.core.impl.arbitrary_precision;

import info.trekto.jos.core.ForceCalculator;
import info.trekto.jos.core.Simulation;
import info.trekto.jos.core.SimulationLogic;
import info.trekto.jos.core.exceptions.SimulationException;
import info.trekto.jos.core.impl.Iteration;
import info.trekto.jos.core.impl.SimulationProperties;
import info.trekto.jos.core.model.ImmutableSimulationObject;
import info.trekto.jos.core.model.SimulationObject;
import info.trekto.jos.core.model.impl.SimulationObjectImpl;
import info.trekto.jos.core.numbers.Number;
import info.trekto.jos.util.Utils;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import static info.trekto.jos.core.Controller.C;
// import static info.trekto.jos.core.impl.arbitrary_precision.SimulationRecursiveAction.threshold;
import static info.trekto.jos.util.Utils.*;

/**
 * This implementation uses fork/join Java framework introduced in Java 7.
 *
 * @author Trayan Momkov
 * 2017-May-18
 */


public class SimulationAP implements Simulation {
    private static final Logger logger = LoggerFactory.getLogger(SimulationAP.class);
    public static final int PAUSE_SLEEP_MILLISECONDS = 100;
    public static final int SHOW_REMAINING_INTERVAL_SECONDS = 2;

    private final SimulationLogicAP simulationLogic;
    protected SimulationProperties properties;
    private ForceCalculator forceCalculator;
    protected long iterationCounter;

    private List<SimulationObject> objects;
    private List<SimulationObject> auxiliaryObjects;

    private volatile boolean collisionExists;

    public List<Thread> threads = new ArrayList<>();

        public class MyThreadCNW extends Thread {
            private final int initialIndex;
            private final int finalIndex;

            public MyThreadCNW(int initialIndex, int finalIndex) {
                this.initialIndex = initialIndex;
                this.finalIndex = finalIndex;
            }

            @Override
            public void run() {
                try {
                    simulationLogic.calculateNewValues(initialIndex, finalIndex);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }// create threads

    public SimulationAP(SimulationProperties properties) {
        simulationLogic = new SimulationLogicAP(this);
        this.properties = properties;
    }

    public boolean collisionExists(List<SimulationObject> objects) {
        for (SimulationObject o : objects) {
            for (SimulationObject o1 : objects) {
                if (o == o1) {
                    continue;
                }
                // distance between centres
                Number distance = simulationLogic.calculateDistance(o, o1);

                if (distance.compareTo(o.getRadius().add(o1.getRadius())) < 0) {
                    info(logger, String.format("Collision between object A(x:%f, y:%f, r:%f) and B(x:%f, y:%f, r:%f)",
                            o.getX().doubleValue(), o.getY().doubleValue(), o.getRadius().doubleValue(),
                            o1.getX().doubleValue(), o1.getY().doubleValue(), o1.getRadius().doubleValue()));
                    return true;
                }
            }
        }
        return false;
    }

    public boolean duplicateIdExists(List<SimulationObject> objects) {
        Set<String> ids = new HashSet<>();
        for (SimulationObject object : objects) {
            if (!ids.add(object.getId())) {
                return true;
            }
        }
        return false;
    }

    public void doIteration(boolean saveCurrentIterationToFile, long iterationCounter) throws InterruptedException {
        ReentrantLock lock = new ReentrantLock();   // Creamos una instancia de ReentrantLock
        auxiliaryObjects = deepCopy(objects);

        /* Distribute simulation objects per threads and start execution *
        threshold = objects.size() / CORES;
        if (threshold < 20) {
            threshold = 20;
        }
        */
        //new SimulationRecursiveAction(0, objects.size(), this).compute();
        simulationLogic.calculateNewValues(0, objects.size());
        /* Collision */
        CollisionCheckAP collisionCheck = new CollisionCheckAP(0, auxiliaryObjects.size(), this);
        collisionExists = false;
        collisionCheck.checkAllCollisions();
        /* If collision/s exists execute sequentially on a single thread */
        if (collisionExists) {
            simulationLogic.processCollisions(this);
        }

        objects = auxiliaryObjects;

        /* Slow down visualization */
        if (properties.isRealTimeVisualization() && properties.getPlayingSpeed() < 0) {
            Thread.sleep(-properties.getPlayingSpeed());
        }

        if (properties.isSaveToFile() && saveCurrentIterationToFile) {
            C.getReaderWriter().appendObjectsToFile(objects, properties, iterationCounter);
        }

    }

    public void playSimulation(String inputFile) {
        try {
            // Only reset reader pointer. Do not change properties! We want to have the latest changes from the GUI.
            C.getReaderWriter().readPropertiesForPlaying(inputFile);
        } catch (IOException e) {
            error(logger, "Cannot reset input file for playing.", e);
        }
        C.setVisualizer(C.createVisualizer(properties));
        long previousTime = System.nanoTime();
        long previousVisualizationTime = previousTime;
        C.setRunning(true);
        C.setEndText("END.");
        try {
            while (C.getReaderWriter().hasMoreIterations()) {
                if (C.hasToStop()) {
                    doStop();
                    break;
                }
                while (C.isPaused()) {
                    Thread.sleep(PAUSE_SLEEP_MILLISECONDS);
                }
                Iteration iteration = C.getReaderWriter().readNextIteration();
                if (iteration == null) {
                    break;
                }

                if (System.nanoTime() - previousTime >= NANOSECONDS_IN_ONE_SECOND * SHOW_REMAINING_INTERVAL_SECONDS) {
                    previousTime = System.nanoTime();
                    info(logger, "Cycle: " + iteration.getCycle() + ", number of objects: " + iteration.getNumberOfObjects());
                }

                if (properties.getPlayingSpeed() < 0) {
                    /* Slow down */
                    Thread.sleep(-properties.getPlayingSpeed());
                    C.getVisualizer().visualize(iteration);
                    previousVisualizationTime = System.nanoTime();
                } else if ((System.nanoTime() - previousVisualizationTime) / NANOSECONDS_IN_ONE_MILLISECOND >= properties.getPlayingSpeed()) {
                    C.getVisualizer().visualize(iteration);
                    previousVisualizationTime = System.nanoTime();
                }
            }
            info(logger, "End.");
            C.getVisualizer().end();
        } catch (IOException e) {
            error(logger, "Error while reading simulation object.", e);
        } catch (InterruptedException e) {
            error(logger, "Thread interrupted.", e);
        } finally {
            C.setRunning(false);
        }
    }

    protected void doStop() {
        C.setHasToStop(false);
        if (properties.isSaveToFile()) {
            C.getReaderWriter().endFile();
        }
        if (C.getVisualizer() != null) {
            C.setEndText("Stopped!");
            C.getVisualizer().closeWindow();
        }
    }

    @Override
    public void startSimulation() throws SimulationException {
        init(true);

        info(logger, "Start simulation...");
        C.setEndText("END.");
        long startTime = System.nanoTime();
        long previousTime = startTime;
        long previousVisualizationTime = startTime;
        long endTime;
        int numberOfObjects = 0;
        numberOfObjects = getObjects().size();
        int numberOfThreads = getProperties().getNumberOfThreads();
        int numberOfObjectsPerThread = numberOfObjects / numberOfThreads;
        int numberOfObjectsLeft = numberOfObjects % numberOfThreads;
        int from;
        int to;
        for (int i = 0; i < numberOfThreads; i++) {     //calcular particulas por thread
            from = i * numberOfObjectsPerThread;
            to = from + numberOfObjectsPerThread - 1;
            if (numberOfObjectsLeft > 0) {
                to++;
                numberOfObjectsLeft--;
            }
            if (i == numberOfThreads) {     //ultima particula
                to = numberOfObjects + 1;
            }
            MyThreadCNW thread = new MyThreadCNW(from, to);     // create thread
            threads.add(thread);

        }
        C.setRunning(true);
        C.setHasToStop(false);
        try {
            for (Thread thread : threads) {
                thread.start();
            }

            for (long i = 0; properties.isInfiniteSimulation() || i < properties.getNumberOfIterations(); i++) {
                try {
                    if (C.hasToStop()) {
                        doStop();
                        break;
                    }
                    while (C.isPaused()) {
                        Thread.sleep(PAUSE_SLEEP_MILLISECONDS);
                    }

                    iterationCounter = i + 1;

                    if (System.nanoTime() - previousTime >= NANOSECONDS_IN_ONE_SECOND * SHOW_REMAINING_INTERVAL_SECONDS) {
                        showRemainingTime(i, startTime, properties.getNumberOfIterations(), objects.size());
                        previousTime = System.nanoTime();
                    }

                    if (properties.isRealTimeVisualization()) {
                        endTime = System.nanoTime();
                        if (properties.getPlayingSpeed() < 0) {
                            /* Slow down */
                            Thread.sleep(-properties.getPlayingSpeed());
                            C.getVisualizer().visualize(objects, endTime - startTime, iterationCounter);
                            previousVisualizationTime = System.nanoTime();
                        } else if ((System.nanoTime() - previousVisualizationTime) / NANOSECONDS_IN_ONE_MILLISECOND >= properties.getPlayingSpeed()) {
                            C.getVisualizer().visualize(objects, endTime - startTime, iterationCounter);
                            previousVisualizationTime = System.nanoTime();
                        }
                    }


                    Boolean saveCurrentIterationToFile = (i % properties.getSaveEveryNthIteration() == 0);

                    doIteration(saveCurrentIterationToFile, iterationCounter);

                } catch (InterruptedException e) {
                    error(logger, "Concurrency failure. One of the threads interrupted in cycle " + i, e);
                }
            }

            if (properties.isRealTimeVisualization()) {
                C.getVisualizer().end();
            }
            endTime = System.nanoTime();
        } finally {
            C.setRunning(false);
            if (properties.isSaveToFile()) {
                C.getReaderWriter().endFile();
            }
            // Esperamos a que todos los hilos finalicen
            for (Thread threadEnd : threads) {
                try {
                    threadEnd.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        info(logger, "End of simulation. Time: " + nanoToHumanReadable(endTime - startTime));
    }

    public void init(boolean printInfo) throws SimulationException {
        if (printInfo) {
            logger.info("Initialize simulation...");
        }

        switch (properties.getInteractingLaw()) {
            case NEWTON_LAW_OF_GRAVITATION:
                forceCalculator = new NewtonGravityAP();
                break;
            case COULOMB_LAW_ELECTRICALLY:
                throw new NotImplementedException("COULOMB_LAW_ELECTRICALLY is not implemented");
                // break;
            default:
                forceCalculator = new NewtonGravityAP();
                break;
        }

        objects = new ArrayList<>();

        for (SimulationObject simulationObject : properties.getInitialObjects()) {
            objects.add(new SimulationObjectImpl(simulationObject));
        }

        if (duplicateIdExists(objects)) {
            throw new SimulationException("Objects with duplicate IDs exist!");
        }

        if (collisionExists(objects)) {
            throw new SimulationException("Initial collision exists!");
        }
        if (printInfo) {
            logger.info("Done.\n");
            Utils.printConfiguration(this);
        }
    }

    public void initSwitchingFromGpu(List<SimulationObject> currentObjects) {
        logger.info("Switching to CPU - Initialize simulation...");

        switch (properties.getInteractingLaw()) {
            case NEWTON_LAW_OF_GRAVITATION:
                forceCalculator = new NewtonGravityAP();
                break;
            case COULOMB_LAW_ELECTRICALLY:
                throw new NotImplementedException("COULOMB_LAW_ELECTRICALLY is not implemented");
                // break;
            default:
                forceCalculator = new NewtonGravityAP();
                break;
        }

        objects = currentObjects;
        logger.info("Done.\n");

        Utils.printConfiguration(this);
    }

    @Override
    public List<SimulationObject> getObjects() {
        return objects;
    }

    @Override
    public List<SimulationObject> getAuxiliaryObjects() {
        return auxiliaryObjects;
    }

    @Override
    public long getCurrentIterationNumber() {
        return iterationCounter;
    }

    @Override
    public ForceCalculator getForceCalculator() {
        return forceCalculator;
    }

    public SimulationLogicAP getSimulationLogic() {
        return simulationLogic;
    }

    @Override
    public SimulationProperties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(SimulationProperties properties) {
        this.properties = properties;
    }

    @Override
    public Number calculateDistance(ImmutableSimulationObject object, ImmutableSimulationObject object1) {
        return simulationLogic.calculateDistance(object, object1);
    }

    public boolean isCollisionExists() {
        return collisionExists;
    }

    public void upCollisionExists() {
        collisionExists = true;
    }
}