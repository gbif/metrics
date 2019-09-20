/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.gbif.metrics.cube.tile.io;  
@SuppressWarnings("all")
public class LocationAvro extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"LocationAvro\",\"namespace\":\"org.gbif.metrics.cube.tile.io\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"lat\",\"type\":\"double\"},{\"name\":\"lng\",\"type\":\"double\"}]}");
  @Deprecated public int id;
  @Deprecated public double lat;
  @Deprecated public double lng;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return lat;
    case 2: return lng;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Integer)value$; break;
    case 1: lat = (java.lang.Double)value$; break;
    case 2: lng = (java.lang.Double)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   */
  public java.lang.Integer getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(java.lang.Integer value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'lat' field.
   */
  public java.lang.Double getLat() {
    return lat;
  }

  /**
   * Sets the value of the 'lat' field.
   * @param value the value to set.
   */
  public void setLat(java.lang.Double value) {
    this.lat = value;
  }

  /**
   * Gets the value of the 'lng' field.
   */
  public java.lang.Double getLng() {
    return lng;
  }

  /**
   * Sets the value of the 'lng' field.
   * @param value the value to set.
   */
  public void setLng(java.lang.Double value) {
    this.lng = value;
  }

  /** Creates a new LocationAvro RecordBuilder */
  public static org.gbif.metrics.cube.tile.io.LocationAvro.Builder newBuilder() {
    return new org.gbif.metrics.cube.tile.io.LocationAvro.Builder();
  }
  
  /** Creates a new LocationAvro RecordBuilder by copying an existing Builder */
  public static org.gbif.metrics.cube.tile.io.LocationAvro.Builder newBuilder(org.gbif.metrics.cube.tile.io.LocationAvro.Builder other) {
    return new org.gbif.metrics.cube.tile.io.LocationAvro.Builder(other);
  }
  
  /** Creates a new LocationAvro RecordBuilder by copying an existing LocationAvro instance */
  public static org.gbif.metrics.cube.tile.io.LocationAvro.Builder newBuilder(org.gbif.metrics.cube.tile.io.LocationAvro other) {
    return new org.gbif.metrics.cube.tile.io.LocationAvro.Builder(other);
  }
  
  /**
   * RecordBuilder for LocationAvro instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<LocationAvro>
    implements org.apache.avro.data.RecordBuilder<LocationAvro> {

    private int id;
    private double lat;
    private double lng;

    /** Creates a new Builder */
    private Builder() {
      super(org.gbif.metrics.cube.tile.io.LocationAvro.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.gbif.metrics.cube.tile.io.LocationAvro.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing LocationAvro instance */
    private Builder(org.gbif.metrics.cube.tile.io.LocationAvro other) {
            super(org.gbif.metrics.cube.tile.io.LocationAvro.SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = (java.lang.Integer) data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.lat)) {
        this.lat = (java.lang.Double) data().deepCopy(fields()[1].schema(), other.lat);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.lng)) {
        this.lng = (java.lang.Double) data().deepCopy(fields()[2].schema(), other.lng);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'id' field */
    public java.lang.Integer getId() {
      return id;
    }
    
    /** Sets the value of the 'id' field */
    public org.gbif.metrics.cube.tile.io.LocationAvro.Builder setId(int value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'id' field has been set */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'id' field */
    public org.gbif.metrics.cube.tile.io.LocationAvro.Builder clearId() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'lat' field */
    public java.lang.Double getLat() {
      return lat;
    }
    
    /** Sets the value of the 'lat' field */
    public org.gbif.metrics.cube.tile.io.LocationAvro.Builder setLat(double value) {
      validate(fields()[1], value);
      this.lat = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'lat' field has been set */
    public boolean hasLat() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'lat' field */
    public org.gbif.metrics.cube.tile.io.LocationAvro.Builder clearLat() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'lng' field */
    public java.lang.Double getLng() {
      return lng;
    }
    
    /** Sets the value of the 'lng' field */
    public org.gbif.metrics.cube.tile.io.LocationAvro.Builder setLng(double value) {
      validate(fields()[2], value);
      this.lng = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'lng' field has been set */
    public boolean hasLng() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'lng' field */
    public org.gbif.metrics.cube.tile.io.LocationAvro.Builder clearLng() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public LocationAvro build() {
      try {
        LocationAvro record = new LocationAvro();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Integer) defaultValue(fields()[0]);
        record.lat = fieldSetFlags()[1] ? this.lat : (java.lang.Double) defaultValue(fields()[1]);
        record.lng = fieldSetFlags()[2] ? this.lng : (java.lang.Double) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
