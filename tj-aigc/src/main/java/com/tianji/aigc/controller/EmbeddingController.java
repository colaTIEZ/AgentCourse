package com.tianji.aigc.controller;

import cn.hutool.core.collection.CollStreamUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/embedding")
@RequiredArgsConstructor
public class EmbeddingController {

    private final VectorStore vectorStore;
    private final EmbeddingModel embeddingModel;

    @PostMapping
    public void saveVectorStore(@RequestParam("messages") List<String> messages) {
        log.info("保存到向量数据库中，消息数据为：{}", messages);

        List<Document> documents = CollStreamUtil.toList(messages, message -> Document.builder()
                .text(message)
                .build());
        vectorStore.add(documents);
        log.info("向量数据库中保存成功,数量：{}", messages.size());
    }

    @GetMapping
    public EmbeddingResponse embed(@RequestParam("message") String message) {
        return embeddingModel.embedForResponse(List.of(message));
    }

    @DeleteMapping
    public void deleteVectorStore(@RequestParam("ids") List<String> ids) {
        log.info("从向量数据库中删除，ids数据为：{}", ids);
        vectorStore.delete(ids);
        log.info("向量数据库中删除成功,数量：{}", ids.size());
    }

    @GetMapping("/search")
    public List<Document> search(@RequestParam("message") String message) {
        return vectorStore.similaritySearch(SearchRequest.builder()
                .query(message)
                .topK(5)
                .build());
    }

    @GetMapping("/search/all")
    public List<Document> searchAll() {
        return vectorStore.similaritySearch(SearchRequest.builder()
                .query("")
                .topK(999)
                .build());
    }
}
