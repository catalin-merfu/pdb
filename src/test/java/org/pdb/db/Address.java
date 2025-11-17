package org.pdb.db;

import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.Strings;

public class Address implements Comparable<Address> {
	String province;
	String city;
	String streetName;
	String streetType;
	String streetDirection;
	String streetNo;
	String unit;
	
	public Address(String province, String city, String streetName, String streetType, String streetDirection,
			String streetNo, String unit) {

		this.province = province;
		this.city = city;
		this.streetName = streetName;
		this.streetType = streetType;
		this.streetDirection = streetDirection;
		this.streetNo = streetNo;
		this.unit = unit;
	}

	
	@Override
	public int hashCode() {
		return Objects.hash(city, province, streetDirection, streetName, streetNo, streetType, unit);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Address other = (Address) obj;
		return Objects.equals(city, other.city) && Objects.equals(province, other.province)
				&& Objects.equals(streetDirection, other.streetDirection)
				&& Objects.equals(streetName, other.streetName) && Objects.equals(streetNo, other.streetNo)
				&& Objects.equals(streetType, other.streetType) && Objects.equals(unit, other.unit);
	}

	@Override
	public int compareTo(Address o) {
		
		return Stream.<IntSupplier>of(
				() -> Strings.CS.compare(province, o.province),
				() -> Strings.CS.compare(city, o.city),
				() -> Strings.CS.compare(streetName, o.streetName),
				() -> Strings.CS.compare(streetType, o.streetType),
				() -> Strings.CS.compare(streetDirection, o.streetDirection),
				() -> Strings.CS.compare(streetNo, o.streetNo),
				() -> Strings.CS.compare(unit, o.unit)
			).map(IntSupplier::getAsInt).filter(rc -> rc != 0).findFirst().orElse(0);
	}
	
	@Override
	public String toString() {
		
		return Stream.of(
				province,
				city,
				streetName,
				streetType,
				streetDirection,
				streetNo,
				unit
			).collect(Collectors.joining("|"));
	}
}
