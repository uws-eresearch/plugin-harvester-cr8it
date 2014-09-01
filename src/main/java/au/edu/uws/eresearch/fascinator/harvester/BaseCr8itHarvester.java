package au.edu.uws.eresearch.fascinator.harvester;

import java.util.Set;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.common.harvester.impl.GenericHarvester;

/**
 * 
 * @author lloyd harischandra
 */
public class BaseCr8itHarvester extends GenericHarvester
{

	/** Indexer*/
    protected Indexer indexer;

	public BaseCr8itHarvester(String id, String name) {
		super(id, name);
	}

	@Override
	public Set<String> getObjectIdList() throws HarvesterException {
		return null;
	}

	@Override
	public boolean hasMoreObjects() {
		return false;
	}

	@Override
	public void init() throws HarvesterException {
		
	}
	
	public Indexer getIndexer() {
		return indexer;
	}

	public void setIndexer(Indexer indexer) {
		this.indexer = indexer;
	}
    
}
