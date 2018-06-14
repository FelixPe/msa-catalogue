package de.predic8.workshop.catalogue.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.predic8.workshop.catalogue.domain.Article;
import de.predic8.workshop.catalogue.error.NotFoundException;
import de.predic8.workshop.catalogue.event.Operation;
import de.predic8.workshop.catalogue.repository.ArticleRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/articles")
public class CatalogueRestController {
	private ArticleRepository articleRepository;
	private KafkaTemplate<String, Operation> kafka;
	private ObjectMapper mapper;

	public CatalogueRestController(ArticleRepository articleRepository, KafkaTemplate<String, Operation> kafka, ObjectMapper mapper) {
		this.articleRepository = articleRepository;
		this.kafka = kafka;
		this.mapper = mapper;
	}

	@GetMapping
	public List<Article> index() {
		return articleRepository.findAll();
	}

	@GetMapping("/{id}")
	public Article index(@PathVariable String id) {
		Article article = articleRepository.findOne(id);

		if (article == null) {
			throw new NotFoundException();
		}

		return article;
	}

	@PatchMapping("/{id}")
	public ResponseEntity patch(@RequestBody Article articlePatch, @PathVariable String id) throws InterruptedException, ExecutionException, TimeoutException {

		Article article = articleRepository.getOne(id);
		if (article == null) {
			throw new NotFoundException();
		}

		Article articleUpdate = new Article();
		articleUpdate.setUuid(id);
		articleUpdate.setName(articlePatch.getName());
		articleUpdate.setPrice(articlePatch.getPrice());

		Operation op = new Operation("article", Operation.UPDATE, mapper.valueToTree(articleUpdate));
		op.logSend();

		kafka.send("shop", op).get(100, TimeUnit.MILLISECONDS);

		return ResponseEntity.accepted().build();
	}


	@DeleteMapping("/{id}")
	public ResponseEntity delete(@PathVariable String id) throws InterruptedException, ExecutionException, TimeoutException {

		Article article = articleRepository.getOne(id);
		if (article == null) {
			throw new NotFoundException();
		}

		Article articleDelete = new Article();
		articleDelete.setUuid(id);
		articleDelete.setName(article.getName());
		articleDelete.setPrice(article.getPrice());

		Operation op = new Operation("article", Operation.DELETE, mapper.valueToTree(articleDelete));
		op.logSend();

		kafka.send("shop", op).get(100, TimeUnit.MILLISECONDS);

		return ResponseEntity.accepted().build();
	}


	@PostMapping
	public ResponseEntity create(@RequestBody Article article, UriComponentsBuilder uriComponentBuilder) throws URISyntaxException, InterruptedException, ExecutionException, TimeoutException {
	    article.setUuid(UUID.randomUUID().toString());
		System.out.println("article = " + article);

		Operation op = new Operation("article", Operation.CREATE, mapper.valueToTree(article));
		op.logSend();

		kafka.send(new ProducerRecord<>("shop", article.getUuid(), op)).get(100, TimeUnit.MILLISECONDS);
		//articleRepository.saveAndFlush(article);
		URI uri = uriComponentBuilder.path("articles/" + article.getUuid()).build().toUri();
		return ResponseEntity.created(uri).build();
	}

	@GetMapping("/count")
	public long count() {
		return articleRepository.count();
	}
}