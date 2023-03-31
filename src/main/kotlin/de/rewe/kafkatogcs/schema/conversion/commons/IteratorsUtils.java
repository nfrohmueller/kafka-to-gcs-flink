/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package de.rewe.kafkatogcs.schema.conversion.commons;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IteratorsUtils {

  /**
   * Create a list from an iterator
   *
   * @param iterator iterator to convert
   * @param <T> type
   * @return list
   */
  public static <T> List<T> toList(final Iterator<T> iterator) {
    final List<T> list = new ArrayList<>();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return list;
  }

}
