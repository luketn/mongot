package com.xgen.testing.mongot.integration.index.serialization;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExpectedResultUtil {

  public static List<ExpectedResult> hydrateResultsIfPossible(
      List<ExpectedResult> variationResultsOutline, List<ExpectedResultItem> flatHydratedItems) {
    if (flatHydratedItems.isEmpty()) {
      return variationResultsOutline;
    }
    if (getId(flatHydratedItems.getFirst()).isEmpty()) {
      return variationResultsOutline;
    }
    Map<Integer, ExpectedResultItem> idToHydratedItems =
        flatHydratedItems.stream()
            .collect(Collectors.toMap(item -> getId(item).get(), item -> item));

    return getHydratedResults(variationResultsOutline, idToHydratedItems);
  }

  private static List<ExpectedResult> getHydratedResults(
      List<? extends ExpectedResult> variationResultsOutline,
      Map<Integer, ExpectedResultItem> idToHydratedItems) {

    List<ExpectedResult> hydratedResults = new ArrayList<>();

    for (ExpectedResult variationResultOutline : variationResultsOutline) {
      if (ExpectedResult.Type.GROUP.equals(variationResultOutline.getType())) {
        List<ExpectedResultItem> outlineGroupItems = variationResultOutline.asGroup().getResults();
        List<ExpectedResultItem> hydratedGroupItems =
            outlineGroupItems.stream()
                .map(item -> getHydratedItem(idToHydratedItems, item))
                .collect(Collectors.toList());
        ExpectedResultGroup hydratedGroup = new ExpectedResultGroup(hydratedGroupItems);
        hydratedResults.add(hydratedGroup);
      } else {
        ExpectedResultItem hydratedItem =
            getHydratedItem(idToHydratedItems, variationResultOutline.asItem());
        hydratedResults.add(hydratedItem);
      }
    }
    return hydratedResults;
  }

  private static ExpectedResultItem getHydratedItem(
      Map<Integer, ExpectedResultItem> idToHydratedItems, ExpectedResultItem outlineItem) {
    Integer id = getId(outlineItem).get();
    return idToHydratedItems.get(id);
  }

  private static Optional<Integer> getId(ExpectedResultItem resultItem) {
    // cannot hydrate from _id for storedsource since _id may not be unique in returnScope-enabled
    // queries
    return resultItem.getId();
  }
}
