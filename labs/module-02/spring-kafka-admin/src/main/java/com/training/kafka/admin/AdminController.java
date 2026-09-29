package com.training.kafka.admin;

import java.util.List;

import com.training.kafka.admin.ClusterAdminService.ClusterView;
import com.training.kafka.admin.ClusterAdminService.CreateTopicRequest;
import com.training.kafka.admin.ClusterAdminService.GroupLagView;
import com.training.kafka.admin.ClusterAdminService.TopicView;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** REST front end for ClusterAdminService. Errors are handled by ApiErrorHandler. */
@RestController
@RequestMapping("/api")
public class AdminController {

    private final ClusterAdminService service;

    public AdminController(ClusterAdminService service) {
        this.service = service;
    }

    @GetMapping("/cluster")
    public ClusterView cluster() throws Exception {
        return service.describeCluster();
    }

    @GetMapping("/topics")
    public List<String> topics(@RequestParam(defaultValue = "false") boolean internal) throws Exception {
        return service.listTopics(internal);
    }

    @GetMapping("/topics/{name}")
    public TopicView topic(@PathVariable String name) throws Exception {
        return service.describeTopic(name);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public TopicView create(@RequestBody CreateTopicRequest request) throws Exception {
        return service.createTopic(request);
    }

    @PatchMapping("/topics/{name}/partitions")
    public TopicView addPartitions(@PathVariable String name, @RequestParam int count) throws Exception {
        return service.increasePartitions(name, count);
    }

    @DeleteMapping("/topics/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name) throws Exception {
        service.deleteTopic(name);
    }

    @GetMapping("/groups/{groupId}")
    public GroupLagView group(@PathVariable String groupId) throws Exception {
        return service.groupLag(groupId);
    }
}
