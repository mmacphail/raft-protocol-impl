package eu.macphail.raft.component;

import eu.macphail.raft.entity.Log;
import eu.macphail.raft.entity.LogPK;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface LogRepository extends CrudRepository<Log, LogPK> {

    List<Log> findByNodeIdOrderByLogOffset(int nodeId);

    @Modifying
    @Query("delete from Log l where l.nodeId = :nodeId and l.logOffset > :index")
    void truncateLog(@Param("nodeId") int nodeId, @Param("index") int index);
}
