package dk.sdu.mmmi.cfei.epwformat;

import java.util.Arrays;
import java.util.stream.Collectors;

class GroundTemperature {

    public GroundTemperature(double groundTemperatureDepth, double depthSoilConductivity, double depthSoilDensity, double depthSoilSpecificHeat, double depthJanuaryAverageGroundTemperature, double depthFebruaryAverageGroundTemperature, double depthMarchAverageGroundTemperature, double depthAprilAverageGroundTemperature, double depthMayAverageGroundTemperature, double depthJuneAverageGroundTemperature, double depthJulyAverageGroundTemperature, double depthAugustAverageGroundTemperature, double depthSeptemberAverageGroundTemperature, double depthOctoberAverageGroundTemperature, double depthNovemberAverageGroundTemperature, double depthDecemberAverageGroundTemperature) {
        this.groundTemperatureDepth = groundTemperatureDepth;
        this.depthSoilConductivity = depthSoilConductivity;
        this.depthSoilDensity = depthSoilDensity;
        this.depthSoilSpecificHeat = depthSoilSpecificHeat;
        this.depthJanuaryAverageGroundTemperature = depthJanuaryAverageGroundTemperature;
        this.depthFebruaryAverageGroundTemperature = depthFebruaryAverageGroundTemperature;
        this.depthMarchAverageGroundTemperature = depthMarchAverageGroundTemperature;
        this.depthAprilAverageGroundTemperature = depthAprilAverageGroundTemperature;
        this.depthMayAverageGroundTemperature = depthMayAverageGroundTemperature;
        this.depthJuneAverageGroundTemperature = depthJuneAverageGroundTemperature;
        this.depthJulyAverageGroundTemperature = depthJulyAverageGroundTemperature;
        this.depthAugustAverageGroundTemperature = depthAugustAverageGroundTemperature;
        this.depthSeptemberAverageGroundTemperature = depthSeptemberAverageGroundTemperature;
        this.depthOctoberAverageGroundTemperature = depthOctoberAverageGroundTemperature;
        this.depthNovemberAverageGroundTemperature = depthNovemberAverageGroundTemperature;
        this.depthDecemberAverageGroundTemperature = depthDecemberAverageGroundTemperature;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.groundTemperatureDepth) ^ (Double.doubleToLongBits(this.groundTemperatureDepth) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthSoilConductivity) ^ (Double.doubleToLongBits(this.depthSoilConductivity) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthSoilDensity) ^ (Double.doubleToLongBits(this.depthSoilDensity) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthSoilSpecificHeat) ^ (Double.doubleToLongBits(this.depthSoilSpecificHeat) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthJanuaryAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthJanuaryAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthFebruaryAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthFebruaryAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthMarchAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthMarchAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthAprilAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthAprilAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthMayAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthMayAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthJuneAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthJuneAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthJulyAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthJulyAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthAugustAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthAugustAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthSeptemberAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthSeptemberAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthOctoberAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthOctoberAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthNovemberAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthNovemberAverageGroundTemperature) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.depthDecemberAverageGroundTemperature) ^ (Double.doubleToLongBits(this.depthDecemberAverageGroundTemperature) >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GroundTemperature other = (GroundTemperature) obj;
        if (Double.doubleToLongBits(this.groundTemperatureDepth) != Double.doubleToLongBits(other.groundTemperatureDepth)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthSoilConductivity) != Double.doubleToLongBits(other.depthSoilConductivity)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthSoilDensity) != Double.doubleToLongBits(other.depthSoilDensity)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthSoilSpecificHeat) != Double.doubleToLongBits(other.depthSoilSpecificHeat)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthJanuaryAverageGroundTemperature) != Double.doubleToLongBits(other.depthJanuaryAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthFebruaryAverageGroundTemperature) != Double.doubleToLongBits(other.depthFebruaryAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthMarchAverageGroundTemperature) != Double.doubleToLongBits(other.depthMarchAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthAprilAverageGroundTemperature) != Double.doubleToLongBits(other.depthAprilAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthMayAverageGroundTemperature) != Double.doubleToLongBits(other.depthMayAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthJuneAverageGroundTemperature) != Double.doubleToLongBits(other.depthJuneAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthJulyAverageGroundTemperature) != Double.doubleToLongBits(other.depthJulyAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthAugustAverageGroundTemperature) != Double.doubleToLongBits(other.depthAugustAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthSeptemberAverageGroundTemperature) != Double.doubleToLongBits(other.depthSeptemberAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthOctoberAverageGroundTemperature) != Double.doubleToLongBits(other.depthOctoberAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthNovemberAverageGroundTemperature) != Double.doubleToLongBits(other.depthNovemberAverageGroundTemperature)) {
            return false;
        }
        if (Double.doubleToLongBits(this.depthDecemberAverageGroundTemperature) != Double.doubleToLongBits(other.depthDecemberAverageGroundTemperature)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.asList(groundTemperatureDepth,
                depthSoilConductivity,
                depthSoilDensity,
                depthSoilSpecificHeat,
                depthJanuaryAverageGroundTemperature,
                depthFebruaryAverageGroundTemperature,
                depthMarchAverageGroundTemperature,
                depthAprilAverageGroundTemperature,
                depthMayAverageGroundTemperature,
                depthJuneAverageGroundTemperature,
                depthJulyAverageGroundTemperature,
                depthAugustAverageGroundTemperature,
                depthSeptemberAverageGroundTemperature,
                depthOctoberAverageGroundTemperature,
                depthNovemberAverageGroundTemperature,
                depthDecemberAverageGroundTemperature).stream()
                .map(Epw::doubleToString)
                .collect(Collectors.joining(","));
    }

    public final double groundTemperatureDepth;
    public final double depthSoilConductivity;
    public final double depthSoilDensity;
    public final double depthSoilSpecificHeat;
    public final double depthJanuaryAverageGroundTemperature;
    public final double depthFebruaryAverageGroundTemperature;
    public final double depthMarchAverageGroundTemperature;
    public final double depthAprilAverageGroundTemperature;
    public final double depthMayAverageGroundTemperature;
    public final double depthJuneAverageGroundTemperature;
    public final double depthJulyAverageGroundTemperature;
    public final double depthAugustAverageGroundTemperature;
    public final double depthSeptemberAverageGroundTemperature;
    public final double depthOctoberAverageGroundTemperature;
    public final double depthNovemberAverageGroundTemperature;
    public final double depthDecemberAverageGroundTemperature;
}
