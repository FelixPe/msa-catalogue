package de.predic8.workshop.catalogue.repository;

import de.predic8.workshop.catalogue.domain.Article;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ArticleRepository extends JpaRepository<Article, String> {
}
