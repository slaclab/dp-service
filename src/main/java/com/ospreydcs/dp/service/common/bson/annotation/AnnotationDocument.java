package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.util.*;

public class AnnotationDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String ownerId;
    private List<String> dataSetIds;
    private String name;
    private List<String> annotationIds;
    private String comment;
    private String calculationsId;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public List<String> getDataSetIds() {
        return dataSetIds;
    }

    public void setDataSetIds(List<String> dataSetIds) {
        this.dataSetIds = dataSetIds;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getAnnotationIds() {
        return annotationIds;
    }

    public void setAnnotationIds(List<String> annotationIds) {
        this.annotationIds = annotationIds;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getCalculationsId() {
        return calculationsId;
    }

    public void setCalculationsId(String calculationsId) {
        this.calculationsId = calculationsId;
    }

    public static AnnotationDocument fromSaveAnnotationRequest(
            final SaveAnnotationRequest request,
            String calculationsDocumentId
    ) {
        final AnnotationDocument document = new AnnotationDocument();

        // set request fields in document
        document.setOwnerId(request.getOwnerId());
        document.setDataSetIds(request.getDataSetIdsList());
        document.setName(request.getName());
        document.setAnnotationIds(request.getAnnotationIdsList());
        document.setComment(request.getComment());

        // only set tags if specified in request
        if (request.getTagsCount() > 0) {
            document.setTags(request.getTagsList());
        }

        // only set attributes if specified in request
        if (request.getAttributesCount() > 0) {
            final Map<String, String> attributeMap =
                    AttributesUtility.attributeMapFromList(request.getAttributesList());
            document.setAttributes(attributeMap);
        }

        if (calculationsDocumentId != null) {
            document.setCalculationsId(calculationsDocumentId);
        }

        return document;
    }

    public QueryAnnotationsResponse.AnnotationsResult.Annotation toAnnotation(
            List<DataSetDocument> dataSetDocuments, CalculationsDocument calculationsDocument) throws DpException {

        QueryAnnotationsResponse.AnnotationsResult.Annotation.Builder annotationBuilder =
                QueryAnnotationsResponse.AnnotationsResult.Annotation.newBuilder();

        annotationBuilder.setId(this.getId().toString());
        annotationBuilder.setOwnerId(this.getOwnerId());
        annotationBuilder.addAllDataSetIds(this.getDataSetIds());
        annotationBuilder.setName(this.getName());
        annotationBuilder.addAllAnnotationIds(this.getAnnotationIds());
        annotationBuilder.setComment(this.getComment());

        // only set tags if specified in document
        if (this.getTags() != null) {
            annotationBuilder.addAllTags(this.getTags());
        }

        // only set attributes if specified in document
        if (this.getAttributes() != null) {
            annotationBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributes()));
        }

        // add content of related datasets
        for (DataSetDocument dataSetDocument : dataSetDocuments) {
            annotationBuilder.addDataSets(dataSetDocument.toDataSet());
        }

        // add calculations content
        if (calculationsDocument != null) {
            annotationBuilder.setCalculations(calculationsDocument.toCalculations());
        }

        return annotationBuilder.build();
    }

    public List<String> diffSaveAnnotationRequest(final SaveAnnotationRequest request) {

        final List<String> diffs = new ArrayList<>();

        // diff ownerId
        if (! Objects.equals(request.getOwnerId(), this.getOwnerId())) {
            final String msg = 
                    "ownerId mismatch: " + this.getOwnerId()
                    + " expected: " + request.getOwnerId();
            diffs.add(msg);
        }

        // diff dataSetIds list
        final Collection<String> dataSetIdsDisjunction = 
                CollectionUtils.disjunction(request.getDataSetIdsList(), this.getDataSetIds());
        if ( ! dataSetIdsDisjunction.isEmpty()) {
            final String msg =
                    "dataSetIds mismatch: " + this.getDataSetIds()
                    + " disjunction: " + dataSetIdsDisjunction;
        }
        
        // diff name
        if ( ! Objects.equals(request.getName(), this.getName())) {
            final String msg = "name mismatch: " + this.getName() + " expected: " + request.getName();
            diffs.add(msg);
        }

        // diff annotationIds list
        final Collection<String> annotationIdsDisjunction =
                CollectionUtils.disjunction(request.getAnnotationIdsList(), this.getAnnotationIds());
        if ( ! annotationIdsDisjunction.isEmpty()) {
            final String msg =
                    "annotationIds mismatch: " + this.getAnnotationIds()
                            + " disjunction: " + annotationIdsDisjunction;
        }

        // diff comment
        if ( ! Objects.equals(request.getComment(), this.getComment())) {
            final String msg = 
                    "comment mismatch: " + this.getComment() + " expected: " + request.getComment();
            diffs.add(msg);
        }

        // diff tags list
        if (this.getTags() != null) {
            final Collection<String> tagsDisjunction =
                    CollectionUtils.disjunction(request.getTagsList(), this.getTags());
            if (!tagsDisjunction.isEmpty()) {
                final String msg =
                        "tags mismatch: " + this.getTags()
                                + " disjunction: " + tagsDisjunction;
                diffs.add(msg);
            }
        } else {
            if (request.getTagsCount() > 0) {
                final String msg = "tags mismatch: null expected: " + request.getTagsList();
                diffs.add(msg);
            }
        }
        
        // diff attributes
        if (this.getAttributes() != null) {
            final Collection<Attribute> attributesDisjunction =
                    CollectionUtils.disjunction(
                            request.getAttributesList(),
                            AttributesUtility.attributeListFromMap(this.getAttributes()));
            if (!attributesDisjunction.isEmpty()) {
                final String msg =
                        "attributes mismatch: " + this.getAttributes()
                                + " disjunction: " + attributesDisjunction;
                diffs.add(msg);
            }
        } else {
            if (request.getAttributesCount() > 0) {
                final String msg = "attributes mismatch: null expected: " + request.getAttributesList();
                diffs.add(msg);
            }
        }

        return diffs;
    }

}
