/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Collection Value. */
@RequiredArgsConstructor
public class ExprCollectionValue extends AbstractExprValue {
  private final List<ExprValue> valueList;

  @Override
  public Object value() {
    List<Object> results = new ArrayList<>();
    for (ExprValue exprValue : valueList) {
      results.add(exprValue.value());
    }
    return results;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.ARRAY;
  }

  @Override
  public List<ExprValue> collectionValue() {
    return valueList;
  }

  @Override
  public String toString() {
    return valueList.stream().map(Object::toString).collect(Collectors.joining(", ", "[", "]"));
  }

  @Override
  public boolean equal(ExprValue o) {
    if (!(o instanceof ExprCollectionValue)) {
      return false;
    } else {
      ExprCollectionValue other = (ExprCollectionValue) o;
      Iterator<ExprValue> thisIterator = this.valueList.iterator();
      Iterator<ExprValue> otherIterator = other.valueList.iterator();

      while (thisIterator.hasNext() && otherIterator.hasNext()) {
        ExprValue thisEntry = thisIterator.next();
        ExprValue otherEntry = otherIterator.next();
        if (!thisEntry.equals(otherEntry)) {
          return false;
        }
      }
      return !(thisIterator.hasNext() || otherIterator.hasNext());
    }
  }

  /**
   * Compare arrays lexicographically, element by element. If all compared elements are equal, the
   * shorter array is considered less than the longer one.
   */
  @Override
  public int compare(ExprValue other) {
    List<ExprValue> otherList = other.collectionValue();
    int minSize = Math.min(valueList.size(), otherList.size());
    for (int i = 0; i < minSize; i++) {
      int cmp = valueList.get(i).compareTo(otherList.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(valueList.size(), otherList.size());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(valueList);
  }
}
