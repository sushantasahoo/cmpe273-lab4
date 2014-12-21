package edu.sjsu.cmpe.cache.repository;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import edu.sjsu.cmpe.cache.domain.Entry;

public class InMemoryCache implements CacheInterface {
    /** In-memory map cache. (Key, Value) -> (Key, Entry) */
    private final ConcurrentHashMap<Long, Entry> inMemoryMap;

    public InMemoryCache(ConcurrentHashMap<Long, Entry> entries) {
        inMemoryMap = entries;
    }

    @Override
    public Entry save(Entry newEntry) {
    	System.out.println("InMemoryCache: Saving entry "+newEntry.getKey()+" "+newEntry.getValue());
        checkNotNull(newEntry, "newEntry instance must not be null");
        inMemoryMap.put(newEntry.getKey(), newEntry);

        return newEntry;
    }

    @Override
    public Entry get(Long key) {
        checkArgument(key > 0,
                "Key was %s but expected greater than zero value", key);
        //System.out.println("Server side get for key 1: "+inMemoryMap.get(key).getValue());
        return inMemoryMap.get(key);
    }

    @Override
    public List<Entry> getAll() {
        return new ArrayList<Entry>(inMemoryMap.values());
    }

	@Override
	public Boolean delete(Long key) {
		System.out.println("In server, deleting key "+key);
		inMemoryMap.remove(key);
		return true;
	}
}
