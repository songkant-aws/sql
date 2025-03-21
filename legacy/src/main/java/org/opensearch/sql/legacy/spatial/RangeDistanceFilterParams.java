/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.spatial;

/** Created by Eliran on 15/8/2015. */
public class RangeDistanceFilterParams extends DistanceFilterParams {
  private final String distanceTo;

  public RangeDistanceFilterParams(String distanceFrom, String distanceTo, Point from) {
    super(distanceFrom, from);
    this.distanceTo = distanceTo;
  }

  public String getDistanceTo() {
    return distanceTo;
  }

  public String getDistanceFrom() {
    return this.getDistance();
  }
}
