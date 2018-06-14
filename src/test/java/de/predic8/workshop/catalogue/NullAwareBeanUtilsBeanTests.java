package de.predic8.workshop.catalogue;

import de.predic8.workshop.catalogue.domain.Article;
import de.predic8.workshop.catalogue.event.NullAwareBeanUtilsBean;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

public class NullAwareBeanUtilsBeanTests {
	@Test
	public void testNullAwareBean() throws InvocationTargetException, IllegalAccessException {
		Article orig = new Article("123", "abc", new BigDecimal(1.99));
		Article dst = new Article("123", null, null);

		new NullAwareBeanUtilsBean().copyProperties(dst, orig);

		assertThat(dst).hasFieldOrPropertyWithValue("name", "abc");
	}
}
