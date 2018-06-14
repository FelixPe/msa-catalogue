package de.predic8.workshop.catalogue.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.predic8.workshop.catalogue.domain.Article;
import de.predic8.workshop.catalogue.repository.ArticleRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;

import static de.predic8.workshop.catalogue.event.Operation.CREATE;
import static de.predic8.workshop.catalogue.event.Operation.UPDATE;
import static de.predic8.workshop.catalogue.event.Operation.DELETE;

@Service
public class ShopListener {
	private final ObjectMapper mapper;
	private final ArticleRepository articleRepository;
	private final NullAwareBeanUtilsBean beanUtils;

	public ShopListener(ObjectMapper mapper, ArticleRepository articleRepository, NullAwareBeanUtilsBean beanUtils) {
		this.mapper = mapper;
		this.articleRepository = articleRepository;
		this.beanUtils = beanUtils;
	}

	@KafkaListener(topics = "shop")
	public void listen(Operation op) throws InvocationTargetException, IllegalAccessException {
		if (op.getBo() == null || op.getObject() == null) return;
		op.logReceive();
		if(op.getBo().equals("article")) {
			Article article = mapper.convertValue(op.getObject(), Article.class);
			if (article == null) return;
			switch (op.getAction()) {
				case UPDATE:
					Article articleToUpdate = articleRepository.findOne(article.getUuid());
					beanUtils.copyProperties(articleToUpdate,article);
					articleRepository.saveAndFlush(articleToUpdate);
					break;
				case CREATE:
					articleRepository.saveAndFlush(article);
					break;
                case DELETE:
                    articleRepository.delete(article.getUuid());
                    break;
			}
		}

	}
}