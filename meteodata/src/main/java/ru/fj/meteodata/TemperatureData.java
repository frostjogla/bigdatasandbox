package ru.fj.meteodata;

public class TemperatureData {

    String stationName;
    int temperature;
    int year;

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((stationName == null) ? 0 : stationName.hashCode());
        result = prime * result + temperature;
        result = prime * result + year;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TemperatureData other = (TemperatureData) obj;
        if (stationName == null) {
            if (other.stationName != null)
                return false;
        } else if (!stationName.equals(other.stationName))
            return false;
        if (temperature != other.temperature)
            return false;
        if (year != other.year)
            return false;
        return true;
    }

}
