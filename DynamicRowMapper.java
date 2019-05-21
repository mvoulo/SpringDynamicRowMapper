package com.devops.helloworld.utils;


import com.google.common.base.CaseFormat;
import org.springframework.jdbc.support.JdbcUtils;

import java.beans.Statement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Use with the Spring {@link org.springframework.jdbc.core.JdbcTemplate}
 * This class allows for automatic mapping of a database result row to an object instead of creating a custom mapper/using hibernate etc
 * <p>
 * Data is mapped to the specified object using reflection to find a setter method name that matches the column (or alias) name.
 * <p>
 * The standard configuration for database is underscored variables. ex: SOME_VARIABLE
 * <p>
 * Generally Java variables are camel case. ex: someVariable
 * <p>
 * By default this row mapper will convert the column/alias when looking for a setter method.
 * ex: column SOME_VARIABLE will look for a setter called setSomeVariable
 * <p>
 * When providing a class for the mapper to map to, the provided class must have a default public constructor so it can
 * have an instance created. Providing an abstract class, interface or class without a public default constructor will result in an error.
 * <p>
 * However you may provide an already instantiated object that is an abstract class, interface or does not have a public default constructor.
 * When providing a pre-instantiated object, if multiple rows are returned an error will be thrown.
 *
 * @param <T> The class that will be mapped
 */
public class DynamicRowMapper<T> {
  private final static Logger LOGGER = Logger.getLogger(DynamicRowMapper.class.getName());

  //The class to be mapped
  private Class<T> mappedClass;

  //Object that will have values mapped to
  private T mappedObject;

  //Setter methods extracted from the mappedClass
  private Map<String, Method> mappedMethods;

  /**
   * Initializes based on a class.
   * The object of the class will be created automatically using the default constructor.
   * If no default constructor is available an error is thrown.
   *
   * @param mappedClass
   * @throws UnsupportedOperationException
   */
  public DynamicRowMapper(Class<T> mappedClass) throws UnsupportedOperationException {
    this.mappedObject = createInstance(mappedClass);
    this.mappedClass = mappedClass;
    initialize();
  }

  /**
   * Uses an existing object to map values to.
   *
   * @param mappedObject
   */
  public DynamicRowMapper(T mappedObject) {
    this.mappedObject = mappedObject;
    this.mappedClass = (Class<T>) mappedObject.getClass();
    initialize();
  }

  private T createInstance(Class<T> classToCreateInstance) throws UnsupportedOperationException {
    try {
      return classToCreateInstance.newInstance();
    } catch (IllegalAccessException | InstantiationException e) {
      throw new UnsupportedOperationException("Failed to create instance of class " + classToCreateInstance.getName() + "./n " +
                                              "Ensure that there is an empty constructor and that it is public.");
    }
  }

  private void initialize() {
    this.mappedMethods = new HashMap<>();
    this.populateSetterMethods(mappedClass);
  }

  /**
   * Recursive function to get the declared public setter methods as well as any methods in the super classes.
   *
   * @param mappedClass
   * @return
   */
  private void populateSetterMethods(Class mappedClass) {
    if (hasSuperClass(mappedClass)) {
      populateSetterMethods(mappedClass.getSuperclass());
    }

    for (Method method : mappedClass.getDeclaredMethods()) {
      if (isSetter(method)) {
        this.mappedMethods.put(method.getName().toUpperCase(), method);
      }
    }
  }

  /**
   * Checks if the provided class has a super class and it is not a java.lang.Object.
   *
   * @param mappedClass
   * @return
   */
  private boolean hasSuperClass(Class<T> mappedClass) {
    return mappedClass.getSuperclass() != null && mappedClass.getSuperclass() == Object.class;
  }

  /**
   * Checks if the provided method is an acceptable setter method.
   * Criteria for an acceptable setter method:
   * <p>
   * Must be public
   * Does not return
   * One parameter
   * Starts with "set" followed by an upper case letter
   *
   * @param method
   * @return
   */
  private boolean isSetter(Method method) {
    return Modifier.isPublic(method.getModifiers()) &&
           method.getReturnType().equals(void.class) &&
           method.getParameterTypes().length == 1 &&
           method.getName().matches("^set[A-Z].*");
  }

  /**
   * This is the portion that is executed within the JDBCTemplate.
   * If there is an issue with code, start here.
   *
   * @param rs
   * @param rowNumber
   * @return
   */
  public T mapRow(ResultSet rs, int rowNumber) {

    try {
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();

      for (int index = 1; index <= columnCount; index++) {

        String column = getColumnName(rsmd, index);
        Object value = getColumnValue(rs, index);

        SetterHelper helper = new SetterHelper(column, mappedMethods);

        if (helper.exists) {
          setValue(helper, value);
        }
      }

    } catch (Exception e) {
      LOGGER.log(Level.WARNING, e.getMessage());
    }

    return mappedObject;
  }

  private void setValue(SetterHelper setterUtil, Object value) throws Exception {

    if (skipSettingValue(value, setterUtil.parameterType)) {
      return;
    }

    if (isLocalDate(setterUtil.parameterType)) {
      value = convertToLocalDate(setterUtil.parameterType, value);
    }

    Statement stmt = new Statement(mappedObject, setterUtil.method.getName(), new Object[]{value});
    stmt.execute();
  }

  /**
   * Gets the column name or alias
   *
   * @param rsmd
   * @param index
   * @return
   * @throws SQLException
   */
  private String getColumnName(ResultSetMetaData rsmd, int index) throws SQLException {
    return JdbcUtils.lookupColumnName(rsmd, index);
  }

  private Object getColumnValue(ResultSet rs, int index) throws SQLException {
    return JdbcUtils.getResultSetValue(rs, index);
  }

  private boolean skipSettingValue(Object value, Class<?> parameterType) {
    return parameterType.isPrimitive() && value == null;
  }

  private boolean isLocalDate(Class<?> clazz) {
    return clazz != Object.class && (clazz.isAssignableFrom(LocalDate.class) || clazz.isAssignableFrom(LocalDateTime.class));
  }

  /**
   * If the field is a localdate, convert to the database field to a date first before setting as localdate
   *
   * @return
   */
  private Object convertToLocalDate(Class<?> clazz, Object value) {
    if (value == null) {
      return null;
    }

    if (clazz.isAssignableFrom(LocalDateTime.class)) {
      return ((Timestamp) value).toLocalDateTime();
    } else if (clazz.isAssignableFrom(LocalDate.class)) {
      return new java.sql.Date(((Date) value).getTime()).toLocalDate();
    }

    return value;
  }

  public class SetterHelper {
    private String columnName;
    private String setterName;

    private boolean exists;
    private Method method;
    private Class<?> parameterType;

    private SetterHelper(String columnName, Map<String, Method> mappedMethods) {
      this.columnName = columnName;
      convertColumnNameToSetter();

      this.exists = mappedMethods.containsKey(this.setterName);

      if (exists) {
        this.method = mappedMethods.get(this.setterName);
        this.parameterType = method.getParameterTypes()[0];
      }
    }

    private void convertColumnNameToSetter() {
      this.setterName = ("set" + convertColumnNameToCamelCase(this.columnName)).toUpperCase();
    }

    private String convertColumnNameToCamelCase(String columnName) {
      return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, columnName);
    }

  }
}
