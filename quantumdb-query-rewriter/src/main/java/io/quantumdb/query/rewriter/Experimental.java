package io.quantumdb.query.rewriter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(value = { ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR })
public @interface Experimental {

	String value() default "";

}
