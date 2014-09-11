/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.functions;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * User-defined function using a method in a class loaded on the classpath by
 * reflection.
 *
 * This is used when the LANGUAGE of the UDF definition is "class".
 */
final class ReflectionBasedUDF extends AbstractJavaUDF
{
    ReflectionBasedUDF(FunctionName name,
                       List<ColumnIdentifier> argNames,
                       List<AbstractType<?>> argTypes,
                       AbstractType<?> returnType,
                       String language,
                       String body,
                       boolean deterministic)
    throws InvalidRequestException
    {
        super(name, argNames, argTypes, returnType, language, body, deterministic);
    }

    String requiredLanguage()
    {
        return "class";
    }

    Method resolveMethod() throws InvalidRequestException
    {
        Class<?> jReturnType = javaReturnType();
        Class<?>[] paramTypes = javaParamTypes();

        String className;
        String methodName;
        int i = body.indexOf('#');
        if (i != -1)
        {
            methodName = body.substring(i + 1);
            className = body.substring(0, i);
        }
        else
        {
            methodName = name.name;
            className = body;
        }
        try
        {
            Class<?> cls = Class.forName(className, false, Thread.currentThread().getContextClassLoader());

            Method method = cls.getMethod(methodName, paramTypes);

            if (!Modifier.isStatic(method.getModifiers()))
                throw new InvalidRequestException("Method " + className + '.' + methodName + '(' + Arrays.toString(paramTypes) + ") is not static");

            if (!jReturnType.isAssignableFrom(method.getReturnType()))
            {
                throw new InvalidRequestException("Method " + className + '.' + methodName + '(' + Arrays.toString(paramTypes) + ") " +
                                                  "has incompatible return type " + method.getReturnType() + " (not assignable to " + jReturnType + ')');
            }

            return method;
        }
        catch (ClassNotFoundException e)
        {
            throw new InvalidRequestException("Class " + className + " does not exist");
        }
        catch (NoSuchMethodException e)
        {
            throw new InvalidRequestException("Method " + className + '.' + methodName + '(' + Arrays.toString(paramTypes) + ") does not exist");
        }
    }
}
