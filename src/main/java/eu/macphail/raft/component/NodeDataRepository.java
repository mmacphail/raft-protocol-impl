package eu.macphail.raft.component;

import eu.macphail.raft.entity.NodeData;
import org.springframework.data.repository.CrudRepository;

public interface NodeDataRepository extends CrudRepository<NodeData, Integer> {
}
