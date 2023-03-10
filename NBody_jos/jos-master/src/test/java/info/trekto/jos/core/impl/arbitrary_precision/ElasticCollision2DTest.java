package info.trekto.jos.core.impl.arbitrary_precision;

import info.trekto.jos.core.ForceCalculator;
import info.trekto.jos.core.Simulation;
import info.trekto.jos.core.SimulationLogic;
import info.trekto.jos.core.exceptions.SimulationException;
import info.trekto.jos.core.impl.SimulationProperties;
import info.trekto.jos.core.impl.double_precision.SimulationLogicDouble;
import info.trekto.jos.core.impl.single_precision.SimulationLogicFloat;
import info.trekto.jos.core.model.ImmutableSimulationObject;
import info.trekto.jos.core.model.SimulationObject;
import info.trekto.jos.core.model.impl.SimulationObjectImpl;
import info.trekto.jos.core.model.impl.TripleNumber;
import info.trekto.jos.core.numbers.New;
import info.trekto.jos.core.numbers.Number;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static info.trekto.jos.core.impl.arbitrary_precision.ElasticCollision1DTest.*;
import static info.trekto.jos.core.numbers.NumberFactory.NumberType.ARBITRARY_PRECISION;
import static info.trekto.jos.core.numbers.NumberFactoryProxy.*;
import static org.testng.Assert.assertEquals;

public class ElasticCollision2DTest {
    ImmutableSimulationObject o;

    @BeforeClass
    public void init() {
        createNumberFactory(ARBITRARY_PRECISION, PRECISION);

        SimulationObject om = new SimulationObjectImpl();
        om.setMass(ONE);
        om.setVelocity(new TripleNumber(ONE, ZERO, ZERO));
        om.setRadius(ONE);
        om.setX(HALF.negate());
        om.setY(ZERO);
        om.setZ(ZERO);
        o = om;
    }

    @DataProvider(name = "logic_implementations")
    public static Object[][] logicImplementations() {
        return new Object[][]{
                {new SimulationLogicAP(new SimulationAP(new SimulationProperties())), PRECISION - 2},
                {new SimulationLogicDouble(2, 1, 0, 0, false, 1),
                        DOUBLE_PRECISION - 1},
                {new SimulationLogicFloat(2, 1, 0, 0, false, 1),
                        SINGLE_PRECISION - 1}
        };
    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedFromLeftAndTop90Degrees(SimulationLogic simulationLogic, int precision) {
        SimulationObject o1 = new SimulationObjectImpl(o);
        SimulationObject o2 = new SimulationObjectImpl(o);
        o2.setX(ZERO);
        o2.setY(HALF.negate());
        TripleNumber o2Velocity = new TripleNumber(ZERO, ONE, ZERO);
        o2.setVelocity(o2Velocity);

        simulationLogic.processTwoDimensionalCollision(o1, o2, ONE);

        assertEquals(o1.getVelocity(), new TripleNumber(ZERO, ONE, ZERO), error(simulationLogic));
        assertEquals(o2.getVelocity(), new TripleNumber(ONE, ZERO, ZERO), error(simulationLogic));
    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedFromLeftAndBottom90Degrees(SimulationLogic simulationLogic, int precision) {
        SimulationObject o1 = new SimulationObjectImpl(o);
        SimulationObject o2 = new SimulationObjectImpl(o);
        o2.setX(ZERO);
        o2.setY(HALF);
        TripleNumber o2Velocity = new TripleNumber(ZERO, ONE.negate(), ZERO);
        o2.setVelocity(o2Velocity);

        simulationLogic.processTwoDimensionalCollision(o1, o2, ONE);

        assertEquals(o1.getVelocity(), new TripleNumber(ZERO, ONE.negate(), ZERO), error(simulationLogic));
        assertEquals(o2.getVelocity(), new TripleNumber(ONE, ZERO, ZERO), error(simulationLogic));
    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedFromRightAndTop90Degrees(SimulationLogic simulationLogic, int precision) {
        SimulationObject o1 = new SimulationObjectImpl(o);
        o1.setX(HALF);
        o1.setVelocity(new TripleNumber(ONE.negate(), ZERO, ZERO));
        SimulationObject o2 = new SimulationObjectImpl(o);
        o2.setX(ZERO);
        o2.setY(HALF.negate());
        TripleNumber o2Velocity = new TripleNumber(ZERO, ONE, ZERO);
        o2.setVelocity(o2Velocity);

        simulationLogic.processTwoDimensionalCollision(o1, o2, ONE);

        assertEquals(o1.getVelocity(), new TripleNumber(ZERO, ONE, ZERO), error(simulationLogic));
        assertEquals(o2.getVelocity(), new TripleNumber(ONE.negate(), ZERO, ZERO), error(simulationLogic));
    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedFromRightAndBottom90Degrees(SimulationLogic simulationLogic, int precision) {
        SimulationObject o1 = new SimulationObjectImpl(o);
        o1.setX(HALF);
        o1.setVelocity(new TripleNumber(ONE.negate(), ZERO, ZERO));
        SimulationObject o2 = new SimulationObjectImpl(o);
        o2.setX(ZERO);
        o2.setY(HALF);
        TripleNumber o2Velocity = new TripleNumber(ZERO, ONE.negate(), ZERO);
        o2.setVelocity(o2Velocity);

        simulationLogic.processTwoDimensionalCollision(o1, o2, ONE);

        assertEquals(o1.getVelocity(), new TripleNumber(ZERO, ONE.negate(), ZERO), error(simulationLogic));
        assertEquals(o2.getVelocity(), new TripleNumber(ONE.negate(), ZERO, ZERO), error(simulationLogic));
    }
    
    @Test(dataProvider = "logic_implementations")
    public void equalMassDiffSpeedFromLeftAndTop90Degrees(SimulationLogic simulationLogic, int precision) {
        SimulationObject o1 = new SimulationObjectImpl(o);
        SimulationObject o2 = new SimulationObjectImpl(o);
        o2.setX(ZERO);
        o2.setY(HALF.negate());
        TripleNumber o2Velocity = new TripleNumber(ZERO, TWO, ZERO);
        o2.setVelocity(o2Velocity);

        simulationLogic.processTwoDimensionalCollision(o1, o2, ONE);

        assertEquals(o1.getVelocity(), new TripleNumber(HALF.negate(), New.num("1.5"), ZERO), error(simulationLogic));
        assertEquals(o2.getVelocity(), new TripleNumber(New.num("1.5"), HALF, ZERO), error(simulationLogic));
    }
    
    @Test(dataProvider = "logic_implementations")
    public void diffMassDiffSpeedFromLeftAndTop90Degrees(SimulationLogic simulationLogic, int precision) {
        SimulationObject o1 = new SimulationObjectImpl(o);
        SimulationObject o2 = new SimulationObjectImpl(o);
        o2.setX(ZERO);
        o2.setY(HALF.negate());
        TripleNumber o2Velocity = new TripleNumber(ZERO, TWO, ZERO);
        o2.setVelocity(o2Velocity);
        o2.setMass(TWO);

        simulationLogic.processTwoDimensionalCollision(o1, o2, ONE);

//        assertEquals(o1.getVelocity(), new TripleNumber(New.num("-0.87"), New.num("-2.22"), ZERO), error(simulationLogic));
//        assertEquals(o2.getVelocity(), new TripleNumber(New.num("0.93"), New.num("0.89"), ZERO), error(simulationLogic));
    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedOppositeLeftUp(SimulationLogic simulationLogic, int precision) {
        SimulationObject o1 = new SimulationObjectImpl(o);
        o1.setX(New.num("-0.2"));
        o1.setY(New.num("-0.2"));
        SimulationObject o2 = new SimulationObjectImpl(o);
        o2.setX(ONE);
        TripleNumber o2Velocity = new TripleNumber(ZERO, MINUS_ONE, ZERO);
        o1.setX(New.num("0.2"));
        o2.setVelocity(o2Velocity);

        simulationLogic.processTwoDimensionalCollision(o1, o2, ONE);

//        assertEquals(o1.getVelocity(), new TripleNumber(New.num("-0.89"), New.num("-0.46"), ZERO), error(simulationLogic));
//        assertEquals(o2.getVelocity(), new TripleNumber(New.num("0.89"), New.num("0.46"), ZERO), error(simulationLogic));
    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedOppositeLeftDown(SimulationLogic simulationLogic, int precision) {

    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedOppositeTopLeft(SimulationLogic simulationLogic, int precision) {

    }

    @Test(dataProvider = "logic_implementations")
    public void equalMassAndSpeedOppositeTopRight(SimulationLogic simulationLogic, int precision) {

    }

    @Test(dataProvider = "logic_implementations")
    public void diffMassDiffSpeedOppositeLeftUp(SimulationLogic simulationLogic, int precision) {

    }

    @Test(dataProvider = "logic_implementations")
    public void diffMassDiffSpeedCustomAngleOppositeXDirection(SimulationLogic simulationLogic, int precision) {

    }

    @Test(dataProvider = "logic_implementations")
    public void diffMassDiffSpeedCustomAngleSameXDirection(SimulationLogic simulationLogic, int precision) {

    }
}