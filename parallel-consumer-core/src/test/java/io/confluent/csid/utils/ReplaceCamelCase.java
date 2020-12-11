package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.platform.commons.util.StringUtils;
import org.mockito.internal.util.StringUtil;

import java.lang.reflect.Method;

/**
 * Nicer test names.
 * <p>
 * This seems to break some aspects of Ideas test tracking system (i.e. replay failures).
 * <p>
 * https://leeturner.me/posts/building-a-camel-case-junit5-displaynamegenerator/
 */
public class ReplaceCamelCase extends DisplayNameGenerator.Standard {
    public ReplaceCamelCase() {
    }

    public String generateDisplayNameForClass(Class<?> testClass) {
        return this.replaceCapitals(super.generateDisplayNameForClass(testClass));
    }

    public String generateDisplayNameForNestedClass(Class<?> nestedClass) {
        return this.replaceCapitals(super.generateDisplayNameForNestedClass(nestedClass));
    }

    public String generateDisplayNameForMethod(Class<?> testClass, Method testMethod) {
        return super.generateDisplayNameForClass(testClass) + ": " + this.replaceCapitals(testMethod.getName());
    }

    private String replaceCapitals(String name) {
        name = name.replaceAll("([A-Z])", " $1");
        name = name.replaceAll("([0-9]+)", " $1");
        name = name.trim();
        name = capitiliseSentence(name);
        return name;
    }

    private String capitiliseSentence(String sentence) {
        String firstLetterUpper = (sentence.charAt(0) + "").toUpperCase();
        String withoutFirstLetter = sentence.substring(1).toLowerCase();
        return firstLetterUpper + withoutFirstLetter;
    }
}