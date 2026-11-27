/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.exception;

import org.apache.spark.QueryContext;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.util.SparkVersionUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.collection.immutable.Map;

/** Translates exceptions raised by native code into the exceptions thrown to Spark. */
public class NativeErrorTranslator {
  // Velox's message when a char or varchar value is longer than its declared length.
  private static final Pattern CHAR_VARCHAR_LENGTH_EXCEEDED =
      Pattern.compile("Exceeds allowed length limitation: (\\d+)");

  // Velox's message when assert_not_null finds a null in a non-nullable field.
  private static final Pattern NOT_NULL_ASSERT_VIOLATED =
      Pattern.compile("^Reason: Null value appeared in non-nullable field$", Pattern.MULTILINE);

  private NativeErrorTranslator() {}

  /**
   * Translates a Velox type conversion error into a SchemaColumnConvertNotSupportedException.
   * Returns null if the message does not indicate a type conversion error.
   */
  private static RuntimeException translateToSchemaException(String msg) {
    if (msg.contains("not allowed for requested type") || msg.contains("Not a valid type for")) {
      return new SchemaColumnConvertNotSupportedException("unknown", msg, "unknown");
    }
    return null;
  }

  /**
   * Translates a Velox char/varchar length check failure into the exception Spark throws for a
   * value longer than its declared length. Returns null if the message does not indicate one.
   */
  private static RuntimeException translateToCharVarcharLengthException(String msg, Exception e) {
    Matcher matcher = CHAR_VARCHAR_LENGTH_EXCEEDED.matcher(msg);
    if (!matcher.find()) {
      return null;
    }
    String limit = matcher.group(1);
    if (!SparkVersionUtil.gteSpark35()) {
      // Spark 3.4 has no EXCEED_LIMIT_LENGTH error class and throws this plain RuntimeException.
      return new RuntimeException("Exceeds char/varchar type length limitation: " + limit, e);
    }
    // Builds the exception QueryExecutionErrors.exceedMaxLimit builds, with the Velox error as its
    // cause. SparkRuntimeException's cause can only be set through its constructor.
    return new SparkRuntimeException(
        "EXCEED_LIMIT_LENGTH", new Map.Map1<>("limit", limit), e, new QueryContext[0], "");
  }

  /**
   * Translates a Velox assert_not_null failure into the exception Spark throws for a null in a
   * non-nullable field. Returns null if the message does not indicate one.
   */
  private static RuntimeException translateToNotNullAssertViolation(String msg, Exception e) {
    // Spark 3.x throws a NullPointerException whose message names the field instead.
    if (!SparkVersionUtil.gteSpark40() || !NOT_NULL_ASSERT_VIOLATED.matcher(msg).find()) {
      return null;
    }
    // Builds the exception QueryExecutionErrors.notNullAssertViolation builds. Velox's message
    // does not name the field, so the path is "\n\n", the value Spark uses when it has none.
    return new SparkRuntimeException(
        "NOT_NULL_ASSERT_VIOLATION",
        new Map.Map1<>("walkedTypePath", "\n\n"),
        e,
        new QueryContext[0],
        "");
  }

  /**
   * Translates an exception raised by native code into the exception Spark throws for the same
   * error, or into a GlutenException if no translation applies.
   */
  public static RuntimeException translate(Exception e) {
    String msg = findFirstNonNullMessage(e);
    if (msg != null) {
      RuntimeException schemaEx = translateToSchemaException(msg);
      if (schemaEx != null) {
        schemaEx.initCause(e);
        return schemaEx;
      }
      RuntimeException lengthEx = translateToCharVarcharLengthException(msg, e);
      if (lengthEx != null) {
        return lengthEx;
      }
      RuntimeException notNullEx = translateToNotNullAssertViolation(msg, e);
      if (notNullEx != null) {
        return notNullEx;
      }
    }
    // A GlutenException raised by native/nested code is already translated; return it
    // as-is rather than nesting it in another GlutenException. Keep this after every more
    // specific translation: a GlutenException subclass that needs its own translation must
    // be matched above, or this check returns it untranslated.
    if (e instanceof GlutenException) {
      return (GlutenException) e;
    }
    return new GlutenException(e);
  }

  private static String findFirstNonNullMessage(Throwable t) {
    while (t != null) {
      String msg = t.getMessage();
      if (msg != null) {
        return msg;
      }
      t = t.getCause();
    }
    return null;
  }
}
