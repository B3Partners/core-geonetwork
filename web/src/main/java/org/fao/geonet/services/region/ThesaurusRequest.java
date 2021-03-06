package org.fao.geonet.services.region;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;

import jeeves.server.context.ServiceContext;
import jeeves.utils.Log;

import org.fao.geonet.constants.Geonet;
import org.fao.geonet.kernel.KeywordBean;
import org.fao.geonet.kernel.SingleThesaurusFinder;
import org.fao.geonet.kernel.Thesaurus;
import org.fao.geonet.kernel.search.keyword.KeywordRelation;
import org.fao.geonet.kernel.search.keyword.KeywordSearchParamsBuilder;
import org.fao.geonet.kernel.search.keyword.KeywordSearchType;
import org.fao.geonet.util.LangUtils;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class ThesaurusRequest extends Request {

    
    private static final String NO_CATEGORY = "_none_";

    private static final CoordinateReferenceSystem WGS84 = Region.WGS84;

    private WeakHashMap<String, Map<String, String>> categoryTranslations;
    private ServiceContext serviceContext;
    private KeywordSearchParamsBuilder searchBuilder;

    private SingleThesaurusFinder finder;

    private Set<String> localesToLoad;
    
    
    public ThesaurusRequest(ServiceContext context, WeakHashMap<String, Map<String, String>> categoryTranslations, Set<String> localesToLoad, Thesaurus thesaurus) {
        this.localesToLoad = localesToLoad;
        this.serviceContext = context;
        this.categoryTranslations = categoryTranslations;
        this.searchBuilder = new KeywordSearchParamsBuilder(thesaurus.getIsoLanguageMapper());
        for (String lang : localesToLoad) {
            searchBuilder.addLang(lang);
        }
        searchBuilder.addThesaurus(thesaurus.getKey());
        searchBuilder.requireBoundedBy(true);
        this.finder = new SingleThesaurusFinder(thesaurus);
    }

    @Override
    public Request label(String labelParam) {
        searchBuilder.keyword(labelParam, KeywordSearchType.CONTAINS, true);
        return this;
    }

    @Override
    public Request categoryId(String categoryIdParam) {
        if(categoryIdParam.equals(NO_CATEGORY)) {
            categoryIdParam = "";
        }
        searchBuilder.relationship(categoryIdParam, KeywordRelation.BROADER, KeywordSearchType.MATCH, false);
        return this;
    }

    @Override
    public Request maxRecords(int maxRecordsParam) {
        searchBuilder.maxResults(maxRecordsParam);
        return this;
    }

    @Override
    public Collection<Region> execute() throws Exception {
       
        List<KeywordBean> keywords = searchBuilder.build().search(finder);
        List<Region> regions = new ArrayList<Region>(keywords.size());
        for (KeywordBean keywordBean : keywords) {
            String id = keywordBean.getUriCode();
            Map<String, String> unfilteredLabels = keywordBean.getValues();
            Map<String, String> labels = new HashMap<String, String>((int)(unfilteredLabels.size() * 1.25));
            Iterator<Entry<String, String>> iter = unfilteredLabels.entrySet().iterator();
            while(iter.hasNext()) {
                Entry<String, String> next = iter.next();
                if(!next.getValue().trim().isEmpty()) {
                    labels.put(next.getKey(), next.getValue());
                }
            }
            String categoryId = keywordBean.getBroaderRelationship();
            if(categoryId.trim().isEmpty()) {
                categoryId = NO_CATEGORY;
            }
            String categoryLabelKey = categoryId;
            if (categoryLabelKey.equals(NO_CATEGORY)) {
                categoryLabelKey = "none";
            }
            if(categoryLabelKey.indexOf('#') > -1) {
                categoryLabelKey = categoryLabelKey.substring(categoryLabelKey.lastIndexOf('#')+1);
            }
            Map<String, String> categoryLabels = categoryTranslations.get(categoryLabelKey);
            if(categoryLabels == null) {
                categoryLabels = LangUtils.translate(serviceContext, "categories", categoryLabelKey);
                categoryTranslations.put(categoryLabelKey, categoryLabels);
            }
            if (categoryLabels.isEmpty()) {
                for (String loc : localesToLoad) {
                    categoryLabels.put(loc, categoryLabelKey);
                }
            }
            boolean hasGeom = false;
            double west = Double.parseDouble(keywordBean.getCoordWest());
            double east = Double.parseDouble(keywordBean.getCoordEast());
            double north = Double.parseDouble(keywordBean.getCoordNorth());
            double south = Double.parseDouble(keywordBean.getCoordSouth());
            ReferencedEnvelope bbox = new ReferencedEnvelope(west, east, south, north, WGS84);
            Region region = new Region(id, labels, categoryId, categoryLabels, hasGeom, bbox);
            regions.add(region);
        }
        return regions;
    }

    @Override
    public Request id(String regionId) {
        searchBuilder.uri(regionId);
        return this;
    }

}
